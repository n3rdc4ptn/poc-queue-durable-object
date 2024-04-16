import { Hono } from 'hono';
import { DurableObject } from 'cloudflare:workers';

export type Env = {
	MY_DURABLE_OBJECT: DurableObjectNamespace<MyDurableObject>;
	MY_QUEUE: Queue;
	DELAY_PER_QUEUE_EXECUTION: number;
	RANDOMIZE_TO_MAX: boolean;
};

export type QueueMessage = {
	type: 'page' | 'end';
	start_time: number;

	document_id: string;
	page?: number;
	total_pages?: number;
};

/** A Durable Object's behavior is defined in an exported Javascript class */
export class MyDurableObject extends DurableObject<Env> {
	async set_amount_of_pages(amount: number) {
		await this.ctx.storage.put('amount_of_pages', amount);
	}
	async finished_page(page_num: number) {
		const finished_pages: Set<number> = (await this.ctx.storage.get('finished_pages')) || new Set();

		finished_pages.add(page_num);

		await this.ctx.storage.put('finished_pages', finished_pages);
	}
	async is_finished() {
		const finished_pages: Set<number> = (await this.ctx.storage.get('finished_pages')) || new Set();
		const amount_of_pages = (await this.ctx.storage.get('amount_of_pages')) || 0;

		if (finished_pages.size === amount_of_pages) {
			return true;
		}

		return false;
	}
	async get_finished_pages(): Promise<Set<number>> {
		return (await this.ctx.storage.get('finished_pages')) || new Set();
	}
	async delete() {
		await this.ctx.storage.deleteAll();
	}
}

const app = new Hono<{ Bindings: Env }>();

app.get('/send_to_queue', async (c) => {
	// Currently a limit by cloudflare
	const defaultAmount = 100;
	const amount = parseInt(c.req.query('amount') || defaultAmount.toString()) || defaultAmount;

	const document_id = Math.random().toString(36).substring(7);

	const id = await c.env.MY_DURABLE_OBJECT.idFromName(document_id);
	const stub = c.env.MY_DURABLE_OBJECT.get(id);
	await stub.set_amount_of_pages(amount);

	let messages: QueueMessage[] = Array.from({ length: amount }, (_, i) => ({
		type: 'page',
		start_time: Date.now(),
		document_id,
		page: i + 1,
		total_pages: amount,
	}));

	let queue = c.env.MY_QUEUE;

	let messageRequest: MessageSendRequest[] = messages.map((message) => ({
		body: message,
	}));

	await queue.sendBatch(messageRequest);

	return c.text(`Document: ${document_id}. Sent ${amount} messages to the queue!`);
});

app.get('/status', async (c) => {
	const document_id = c.req.query('document_id') || '';

	const id = await c.env.MY_DURABLE_OBJECT.idFromName(document_id);
	const stub = c.env.MY_DURABLE_OBJECT.get(id);

	const finished = await stub.is_finished();
	const finished_pages = await stub.get_finished_pages();

	return c.json({ finished, finished_pages: Array.from(finished_pages) });
});

export default {
	fetch: app.fetch,
	async queue(batch: MessageBatch<QueueMessage>, env: Env): Promise<void> {
		const execution_random_id = Math.random().toString(36).substring(4);

		for (let message of batch.messages) {
			if (message.body.type === 'end') {
				const duration = Date.now() - message.body.start_time;
				console.log('Execution:', execution_random_id, '== Finished document:', message.body.document_id, 'in', duration, 'ms');
				continue;
			}

			if (message.body.type === 'page') {
				const delay_amount = env.RANDOMIZE_TO_MAX ? Math.random() * env.DELAY_PER_QUEUE_EXECUTION : env.DELAY_PER_QUEUE_EXECUTION;
				await new Promise((resolve) => setTimeout(resolve, delay_amount));

				console.log(
					'Execution:',
					execution_random_id,
					'Document:',
					message.body.document_id,
					'Page:',
					message.body.page,
					'/',
					message.body.total_pages
				);

				if (message.body.page) {
					const id = await env.MY_DURABLE_OBJECT.idFromName(message.body.document_id);
					const stub = env.MY_DURABLE_OBJECT.get(id);

					await stub.finished_page(message.body.page);

					if (await stub.is_finished()) {
						await env.MY_QUEUE.send({ type: 'end', document_id: message.body.document_id, start_time: message.body.start_time });
						await stub.delete();
					}
				}
			}
		}
	},
};
