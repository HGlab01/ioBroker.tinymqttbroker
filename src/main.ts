/*
 * Created with @iobroker/create-adapter v2.3.0
 */

// The adapter-core module gives you access to the core ioBroker functions
// you need to create an adapter
import * as utils from '@iobroker/adapter-core';
import { createServer } from 'aedes-server-factory';
import portscanner from 'portscanner';

// eslint-disable-next-line @typescript-eslint/no-require-imports
const jsonExplorer: any = require('iobroker-jsonexplorer');
// eslint-disable-next-line @typescript-eslint/no-require-imports
const { version } = require('../package.json');

class Tinymqttbroker extends utils.Adapter {
    aedes!: any;
    server!: any;

    public constructor(options: Partial<utils.AdapterOptions> = {}) {
        super({
            ...options,
            name: 'tinymqttbroker',
        });
        this.on('ready', this.onReady.bind(this));
        //this.on('stateChange', this.onStateChange.bind(this));
        // this.on('objectChange', this.onObjectChange.bind(this));
        // this.on('message', this.onMessage.bind(this));
        this.on('unload', this.onUnload.bind(this));
        jsonExplorer.init(this, {});
    }

    /**
     * Is called when databases are connected and adapter received configuration.
     */
    private async onReady(): Promise<void> {
        jsonExplorer.sendVersionInfo(version);
        let serverPort = 1883;

        if (this.config.serverPort && this.config.serverPort != 0) {
            serverPort = this.config.serverPort;
        } else {
            const instanceId = `system.adapter.${this.name}.${this.instance}`;
            const objInstance = await this.getForeignObjectAsync(instanceId);
            if (objInstance?.native) {
                const serverPortOld = objInstance.native.option1;
                if (serverPortOld) {
                    this.log.info(`Let's onetime rename config...`);
                    objInstance.native.serverPort = serverPortOld;
                    delete objInstance.native.option1;
                    if (objInstance?.native?.option2) {
                        delete objInstance.native.option2;
                    }
                    await this.setForeignObjectAsync(instanceId, objInstance);
                    serverPort = serverPortOld;
                    this.log.info(`config renamed and saved in instance ${instanceId}`);
                }
            }
        }

        console.log(`Port ${serverPort} is configured`);

        const resultPortScanner = await portscanner.checkPortStatus(serverPort);

        if (resultPortScanner == 'open') {
            this.log.error(`Port ${serverPort} is already in use. Please configure another port in adapter settings!`);
            const end = this.terminate(utils.EXIT_CODES.INVALID_CONFIG_OBJECT);
            return end;
        }

        try {
            const { Aedes }: any = await import('aedes');
            this.aedes = await Aedes.createBroker();
            this.aedes.id = `iobroker_mqtt_broker_${Math.floor(Math.random() * 100000 + 100000)}`;
            this.server = createServer(this.aedes);

            this.server.on('error', (error: any) => {
                if (error?.code === 'EADDRINUSE') {
                    this.log.error(`Port ${serverPort} is already in use. Cannot start MQTT broker.`);
                    const end = this.terminate(utils.EXIT_CODES.INVALID_CONFIG_OBJECT);
                    return end;
                }
                this.log.error(`An error occurred while starting the MQTT broker ${error}`);
                const end = this.terminate(utils.EXIT_CODES.INVALID_CONFIG_OBJECT);
                return end;
            });

            this.server.listen(serverPort, () => {
                this.log.info(`MQTT-broker says: Server ${this.aedes.id} started and listening on port ${serverPort}`);
            });

            // emitted when a client connects to the broker
            this.aedes.on('client', (client: any) => {
                this.log.info(
                    `MQTT-broker says: Client ${client ? client.id : client} connected to broker ${this.aedes.id}`,
                );
            });
            // emitted when a client disconnects from the broker
            this.aedes.on('clientDisconnect', (client: any) => {
                this.log.info(
                    `MQTT-broker says: Client ${client ? client.id : client} disconnected from the broker ${this.aedes.id}`,
                );
            });
            // emitted when a client subscribes to a message topic
            this.aedes.on('subscribe', (subscriptions: any, client: any) => {
                this.log.debug(
                    `MQTT-broker says: Client ${client ? client.id : client} subscribed to topic(s): ${subscriptions.map((s: { topic: any }) => s.topic).join(',')} on broker ${this.aedes.id}`,
                );
            });
            // emitted when a client unsubscribes from a message topic
            this.aedes.on('unsubscribe', (subscriptions: any, client: any) => {
                this.log.debug(
                    `MQTT-broker says: Client ${client ? client.id : client} unsubscribed from topic(s): ${subscriptions.join(',')} on broker ${this.aedes.id}`,
                );
            });

            this.aedes.on('clientError', (client: any, error: any) => {
                this.log.warn(`MQTT-broker says: Client ${client ? client.id : client} error: ${error.message}`);
            });

            this.aedes.on('connectionError', (client: any, error: any) => {
                this.log.warn(
                    `Server forced disconnect for client ${client ? client.id : 'unknown'} due to error: ${error.message}`,
                );
            });

            this.aedes.on('keepaliveTimeout', (client: any) => {
                this.log.warn(`Server is kicking out client ${client.id} due to keep-alive timeout.`);
            });
        } catch (error) {
            this.log.error(`${String(error)}`);
            console.error(`${String(error)}`);
            const end = this.terminate(utils.EXIT_CODES.INVALID_CONFIG_OBJECT);
            return end;
        }
    }

    /**
     * Is called when adapter shuts down - callback has to be called under any circumstances!
     *
     * @param callback is called under any circumstances after stopping the adapter
     */
    private onUnload(callback: () => void): void {
        try {
            this.aedes.close();
            this.server.close();
            this.log.info(`MQTT-broker says: I (${this.aedes.id}) stopped my service. See you soon!`);
            callback();
        } catch {
            callback();
        }
    }

    /**
     * Is called if a subscribed state changes
     *
     * @param id is the ID of the state that changed
     * @param state is the state that changed
     */
    private onStateChange(id: string, state: ioBroker.State | null | undefined): void {
        if (state) {
            // The state was changed
            this.log.info(`state ${id} changed: ${state.val} (ack = ${state.ack})`);
        } else {
            // The state was deleted
            this.log.info(`state ${id} deleted`);
        }
    }

    errorHandling(errorObject: any): void {
        try {
            if (this.log.level != 'debug' && this.log.level != 'silly') {
                if (this.supportsFeature && this.supportsFeature('PLUGINS')) {
                    const sentryInstance = this.getPluginInstance('sentry');
                    if (sentryInstance) {
                        sentryInstance.getSentryObject().captureException(errorObject);
                    }
                }
            }
        } catch (error) {
            console.log(error);
        }
    }

    sendSentry(errorObject: any): void {
        try {
            if (this.supportsFeature && this.supportsFeature('PLUGINS')) {
                const sentryInstance = this.getPluginInstance('sentry');
                if (sentryInstance) {
                    sentryInstance.getSentryObject().captureException(errorObject);
                }
            }
        } catch (error) {
            this.log.error(`Error in function sendSentry(): ${String(error)}`);
        }
    }
}

if (require.main !== module) {
    // Export the constructor in compact mode
    module.exports = (options: Partial<utils.AdapterOptions> | undefined) => new Tinymqttbroker(options);
} else {
    // otherwise start the instance directly
    (() => new Tinymqttbroker())();
}
