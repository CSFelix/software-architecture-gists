type Task<T> = () => Promise<T | undefined>;
type TaskData<T> = { correlationId: string, task: Task<T> };

class DelayService {
  private static readonly BACKOFF: number = 250;
  private static readonly MIN_JITTER: number = 1.00;
  private static readonly MAX_JITTER: number = 2.00;
  private static readonly MAXIMUM_DELAY: number = 5_000;

  private static async sleep(ms: number): Promise<void> {
    return new Promise<void>((resolve, _reject) => {
      setTimeout(() => resolve(), ms);
    });
  }

  public static async sleepWithJitter(retry: number): Promise<void> {
    const delay = this.BACKOFF * 2**(retry - 1);
    const jitter = this.MIN_JITTER + Math.random() * (this.MAX_JITTER - this.MIN_JITTER);
    const ms = Math.min(delay * jitter, this.MAXIMUM_DELAY);
    await this.sleep(ms);
  }
}

class PromiseQueueTaskService<T> {
  //  ****************
  //  ** Attributes **
  //  ****************
  private readonly QUEUE: Array<TaskData<T>>;
  private readonly QUEUE_MAX_SIZE: number;
  private readonly QUEUE_CORRELATION_IDS: Set<string> = new Set();

  private isShuttingDown: boolean = false;

  //  *****************
  //  ** Constructor **
  //  *****************
  constructor(maxSize: number) {
    this.QUEUE = new Array();
    this.QUEUE_MAX_SIZE = maxSize;
  }

  //  **********************
  //  ** Getter & Setters **
  //  **********************
  public getIsShuttingDown(): boolean {
    return this.isShuttingDown;
  }

  public setIsShuttingDown(status: boolean): void {
    this.isShuttingDown = status;

    if (status) {
      this.QUEUE.length = 0;
      this.QUEUE_CORRELATION_IDS.clear();
    }
  }

  //  *************
  //  ** Methods **
  //  *************
  public getQueueSize(): number {
    return this.QUEUE.length;
  }

  public addTask(taskData: TaskData<T>): void {
    if (this.isShuttingDown) throw new Error('PROMISE_PROMISE_QUEUE_TASK_SHUTTING_DOWN');
    if (this.QUEUE_CORRELATION_IDS.has(taskData.correlationId)) throw new Error('PROMISE_QUEUE_TASK_CORRELATION_ID_CONFLICT');
    if (this.QUEUE_CORRELATION_IDS.size >= this.QUEUE_MAX_SIZE) throw new Error('PROMISE_QUEUE_TASK_MAX_SIZE_REACHED');
    this.QUEUE.push(taskData);
    this.QUEUE_CORRELATION_IDS.add(taskData.correlationId);
  }

  public shiftTask(): TaskData<T> | undefined {
    return this.QUEUE.shift();
  }

  public removeTask(correlationId: string): void {
    this.QUEUE_CORRELATION_IDS.delete(correlationId);
  }
}

class WorkerService<T> {
  //  ****************
  //  ** Attributes **
  //  ****************
  private static readonly MAX_RETRIES: number = 5;

  private readonly PROMISE_QUEUE_TASK: PromiseQueueTaskService<T>;
  private readonly CONCURRENCY: number;

  private runningTasks: number = 0;

  //  *****************
  //  ** Constructor **
  //  *****************
  constructor(promiseQueueTask: PromiseQueueTaskService<T>, concurrency: number) {
    this.PROMISE_QUEUE_TASK = promiseQueueTask;
    this.CONCURRENCY = concurrency;
  }

  //  *************
  //  ** Methods **
  //  *************
  private async processTaskWithRetry(currentTask: TaskData<T>): Promise<void> {
    let retries = 0;

    while (true) {
      try {
        await currentTask.task();
        break;
      }
      catch (exception) {
        // TODO:
        // if exception is fatal: throw exception right now
        // else: retry logic

        retries += 1;

        if (retries > WorkerService.MAX_RETRIES) {
          throw new Error('WORKER_SERVICE_TASK_MAXIMUM_RETRIES', { cause: exception });
        }

        await DelayService.sleepWithJitter(retries);
      }
    }
  }

  public async processTask(): Promise<void> {
    if (this.PROMISE_QUEUE_TASK.getQueueSize() === 0) return;
    if (this.runningTasks >= this.CONCURRENCY) return;

    const currentTask = this.PROMISE_QUEUE_TASK.shiftTask();
    if (!currentTask) return;

    try {
      this.runningTasks += 1;
      await this.processTaskWithRetry(currentTask);
    }
    catch (rawException) {
      const exception = rawException as Error;
      console.log(`Worker Service Process Task Exception: ${exception.name} - ${exception.message}`);
      throw rawException;
    }
    finally {
      this.runningTasks -= 1;
      this.PROMISE_QUEUE_TASK.removeTask(currentTask.correlationId);
      
      if (this.PROMISE_QUEUE_TASK.getIsShuttingDown()) return;
      void this.processTask();
    }
  }
}