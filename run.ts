import { IExecutor } from './Executor';
import ITask from './Task';

export default async function run(executor: IExecutor, queue: AsyncIterable<ITask>, maxThreads = 0) {
	maxThreads = Math.max(0, maxThreads);

	/** Интерфейс потока
	 * @typedef targetId
	 * @prop {number} targetId - targetId потока
	 * @prop {boolean} completed - Завершен ли последний промис, в цепочке промисов потока
	 * @prop {Promise<IThread>} [prom] - Ссылка на последний промис цепочки потока
	 */
	interface IThread {
		targetId: number;
		completed: boolean;
		prom?: Promise<IThread>;
	}

	/** Добавляет task в поток thread, для исполнения. Если поток не указан, создает новый. Возвращает поток. */
	function executeTask(task: ITask, thread: IThread = { targetId: -1, completed: false }): IThread {
		thread.targetId = task.targetId;

		async function prom() {
			thread.completed = false;
			await executor.executeTask(task);
			thread.completed = true;
			return thread;
		}
		//Добавляем исполнение таска в цепочку промисов потока
		thread.prom = thread.prom ? thread.prom.finally(prom) : prom();

		return thread;
	}

	//Массив потоков
	let threads: IThread[] = [];
	//Итератор для обхода очереди тасков
	let iterator = queue[Symbol.asyncIterator]();

	while (true) {
		let result = await iterator.next();
		if (result.done) {
			//Если очередь тасков закончилась, ждем пока все потоки завершатся
			await Promise.all(threads.map((thread: IThread) => Promise.resolve(thread.prom)));
			//Если после этого не появились новые таски, завершаем цикл
			result = await iterator.next();
			if (result.done) {
				break;
			}
		}
		let task: ITask = result.value;

		//Ищем подходящий по targetId поток для таска, параллельно убирая все отработавшие потоки
		let foundThread: IThread | undefined;
		let runningThreads: IThread[] = [];
		for (let i = 0; i < threads.length; i++) {
			let thread: IThread = threads[i];
			if (thread.targetId === task.targetId) {
				foundThread = thread;
				runningThreads.push(thread);
			} else if (!thread.completed) {
				runningThreads.push(thread);
			}
		}
		threads = runningThreads;

		if (foundThread) {
			//Если был найден подходящий поток, добавляем в него таск
			executeTask(task, foundThread);
		} else if (maxThreads == 0 || threads.length < maxThreads) {
			//Если подходящий поток не найден, и лимит потоков не исчерпан, добавляем новый поток с таском
			threads.push(executeTask(task));
		} else {
			//Если лимит потоков исчерпан, добавляем таск в первый завершенный поток
			executeTask(task, await Promise.race(threads.map((thread: IThread) => thread.prom)));
		}
	}
}
