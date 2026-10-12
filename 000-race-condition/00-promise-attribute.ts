import type { ChannelModel } from 'amqplib';
import amqp from 'amqplib';

class LoggerService {
	// 	*************
	// 	** Methods **
	// 	*************
	public static logException(message: string, exception: Error): void {
		console.error(`${message}: ${exception.name} - ${exception.message}.`);
	}

	public static logInfo(message: string): void {
		console.info(message);
	}
}

class RabbitMQInstanceService {
	// 	****************
	// 	** Attributes **
	// 	****************
	private static readonly TIMEOUT_MS: number = 30_000;

	private static instance: ChannelModel | undefined;
	private static instancePromise: Promise<ChannelModel> | undefined;

	private static isBlocked: boolean = false;
	private static isShuttingDown: boolean = false;

	// 	*************
	// 	** Methods **
	// 	*************

	//  ---- Bootstrap ----
	private static async createInstance(): Promise<ChannelModel> {
		const { hostname, port, username, password, heartbeat } = instanceConfig;
		const { connectionName } = instanceSocketConfig;

		if (!hostname || !port || !username || !password || !heartbeat || !connectionName) {
			throw new Error('RABBITMQ_SERVICE_MISSING_REQUIRED_PROPERTIES');
		}

		const instance = await amqp.connect({ hostname, port, username, password, heartbeat }, { connectionName });
		return instance;
	}

	private static async createInstanceRace(): Promise<ChannelModel> {
		let timeout: ReturnType<typeof setTimeout> | undefined;

		const timeoutPromise = new Promise<never>((_resolve, reject) => {
			timeout = setTimeout(() => {
				reject(new Error('TIMEOUT'));
			}, this.TIMEOUT_MS);
		});

		const instanceCreationPromise = this.createInstance();

		try {
			await Promise.race([timeoutPromise, instanceCreationPromise]);
		}
		catch (rawException) {
			const exception = rawException as Error;

			LoggerService.logException('RabbitMQ Create Instance Race Exception', exception);

			if (exception.message === 'TIMEOUT') {
				void instanceCreationPromise
					.then(async instance => await instance.close())
					.catch(exception => LoggerService.logException('RabbitMQ Close Timed Out Instance Exception', exception))
					.finally(() => this.instance = undefined);
			}

			throw rawException;
		}
		finally {
			if (timeout) clearTimeout(timeout);
		}
	}

	public static async getOrCreateInstance(): Promise<ChannelModel> {
		if (this.isShuttingDown) throw new Error('RABBITMQ_SERVICE_SHUTTING_DOWN');

		if (this.instance) return this.instance;

		if (!this.instancePromise) {
			this.instancePromise = this.createInstanceRace()
				.then(instance => {
					this.instance = instance;
					this.instance.on('blocked', (reason: string) => this.handleInstanceBlocked(reason));
					this.instance.on('unblocked', () => this.handleInstanceUnblocked());
					this.instance.on('error', (error: Error) => this.handleInstanceError(error));
					this.instance.on('close', () => this.handleInstanceClose().catch(exception => LoggerService.logException('RabbitMQ Handle Instance Close Exception', exception)));
					return instance;
				})
				.catch(exception => {
					LoggerService.logException('RabbitMQ Service Instance Exception', exception);
					throw exception;
				})
				.finally(() => {
					this.instancePromise = undefined;
				});
		}

		return this.instancePromise;
	}

	//  ---- Teardown ----
	public static async closeInstance(): Promise<void> {
		if (this.isShuttingDown) return;

		this.isShuttingDown = true;

		try {
			if (this.instancePromise) await this.instancePromise;
		}
		catch(rawException) {
      const exception = rawException as Error;
			LoggerService.logException('RabbitMQ Close Instance Promise Exception', exception);
		}
		
		try {
			if (this.instance) await this.instance.close();
		}
		catch (rawException) {
      const exception = rawException as Error;
			LoggerService.logException('RabbitMQ Close Instance Exception', exception);
		}

		this.instance = undefined;
		this.instancePromise = undefined;
		this.isBlocked = false;
		this.isShuttingDown = false;
	}

	//  ---- Listeners ----
	private static handleInstanceBlocked(reason: string): void {
		this.isBlocked = true;
		LoggerService.logInfo(`RabbitMQ Instance Blocked: ${reason}`);
	}

	private static handleInstanceUnblocked(): void {
		this.isBlocked = false;
		LoggerService.logInfo('RabbitMQ Instance Unblocked');
	}

	private static handleInstanceError(error: Error): void {
		LoggerService.logException('RabbitMQ Instance Error', error);
	}

	private static async handleInstanceClose(): Promise<void> {
		if (this.isShuttingDown) return;

		this.instance = undefined;
		this.isBlocked = false;
		await this.getOrCreateInstance();
	}
}