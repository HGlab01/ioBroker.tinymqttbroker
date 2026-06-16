"use strict";
var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));
var utils = __toESM(require("@iobroker/adapter-core"));
var import_aedes_server_factory = require("aedes-server-factory");
var import_portscanner = __toESM(require("portscanner"));
const jsonExplorer = require("iobroker-jsonexplorer");
const { version } = require("../package.json");
class Tinymqttbroker extends utils.Adapter {
  aedes;
  server;
  constructor(options = {}) {
    super({
      ...options,
      name: "tinymqttbroker"
    });
    this.on("ready", this.onReady.bind(this));
    this.on("unload", this.onUnload.bind(this));
    jsonExplorer.init(this, {});
  }
  /**
   * Is called when databases are connected and adapter received configuration.
   */
  async onReady() {
    var _a;
    jsonExplorer.sendVersionInfo(version);
    let serverPort = 1883;
    if (this.config.serverPort && this.config.serverPort != 0) {
      serverPort = this.config.serverPort;
    } else {
      const instanceId = `system.adapter.${this.name}.${this.instance}`;
      const objInstance = await this.getForeignObjectAsync(instanceId);
      if (objInstance == null ? void 0 : objInstance.native) {
        const serverPortOld = objInstance.native.option1;
        if (serverPortOld) {
          this.log.info(`Let's onetime rename config...`);
          objInstance.native.serverPort = serverPortOld;
          delete objInstance.native.option1;
          if ((_a = objInstance == null ? void 0 : objInstance.native) == null ? void 0 : _a.option2) {
            delete objInstance.native.option2;
          }
          await this.setForeignObjectAsync(instanceId, objInstance);
          serverPort = serverPortOld;
          this.log.info(`config renamed and saved in instance ${instanceId}`);
        }
      }
    }
    console.log(`Port ${serverPort} is configured`);
    const resultPortScanner = await import_portscanner.default.checkPortStatus(serverPort);
    if (resultPortScanner == "open") {
      this.log.error(`Port ${serverPort} is already in use. Please configure another port in adapter settings!`);
      const end = this.terminate(utils.EXIT_CODES.INVALID_CONFIG_OBJECT);
      return end;
    }
    try {
      const { Aedes } = await Promise.resolve().then(() => __toESM(require("aedes")));
      this.aedes = await Aedes.createBroker();
      this.aedes.id = `iobroker_mqtt_broker_${Math.floor(Math.random() * 1e5 + 1e5)}`;
      this.server = (0, import_aedes_server_factory.createServer)(this.aedes);
      this.server.on("error", (error) => {
        if ((error == null ? void 0 : error.code) === "EADDRINUSE") {
          this.log.error(`Port ${serverPort} is already in use. Cannot start MQTT broker.`);
          const end2 = this.terminate(utils.EXIT_CODES.INVALID_CONFIG_OBJECT);
          return end2;
        }
        this.log.error(`An error occurred while starting the MQTT broker ${error}`);
        const end = this.terminate(utils.EXIT_CODES.INVALID_CONFIG_OBJECT);
        return end;
      });
      this.server.listen(serverPort, () => {
        this.log.info(`MQTT-broker says: Server ${this.aedes.id} started and listening on port ${serverPort}`);
      });
      this.aedes.on("client", (client) => {
        this.log.info(
          `MQTT-broker says: Client ${client ? client.id : client} connected to broker ${this.aedes.id}`
        );
      });
      this.aedes.on("clientDisconnect", (client) => {
        this.log.info(
          `MQTT-broker says: Client ${client ? client.id : client} disconnected from the broker ${this.aedes.id}`
        );
      });
      this.aedes.on("subscribe", (subscriptions, client) => {
        this.log.debug(
          `MQTT-broker says: Client ${client ? client.id : client} subscribed to topic(s): ${subscriptions.map((s) => s.topic).join(",")} on broker ${this.aedes.id}`
        );
      });
      this.aedes.on("unsubscribe", (subscriptions, client) => {
        this.log.debug(
          `MQTT-broker says: Client ${client ? client.id : client} unsubscribed from topic(s): ${subscriptions.join(",")} on broker ${this.aedes.id}`
        );
      });
      this.aedes.on("clientError", (client, error) => {
        this.log.warn(`MQTT-broker says: Client ${client ? client.id : client} error: ${error.message}`);
      });
      this.aedes.on("connectionError", (client, error) => {
        this.log.warn(
          `Server forced disconnect for client ${client ? client.id : "unknown"} due to error: ${error.message}`
        );
      });
      this.aedes.on("keepaliveTimeout", (client) => {
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
  onUnload(callback) {
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
  onStateChange(id, state) {
    if (state) {
      this.log.info(`state ${id} changed: ${state.val} (ack = ${state.ack})`);
    } else {
      this.log.info(`state ${id} deleted`);
    }
  }
  errorHandling(errorObject) {
    try {
      if (this.log.level != "debug" && this.log.level != "silly") {
        if (this.supportsFeature && this.supportsFeature("PLUGINS")) {
          const sentryInstance = this.getPluginInstance("sentry");
          if (sentryInstance) {
            sentryInstance.getSentryObject().captureException(errorObject);
          }
        }
      }
    } catch (error) {
      console.log(error);
    }
  }
  sendSentry(errorObject) {
    try {
      if (this.supportsFeature && this.supportsFeature("PLUGINS")) {
        const sentryInstance = this.getPluginInstance("sentry");
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
  module.exports = (options) => new Tinymqttbroker(options);
} else {
  (() => new Tinymqttbroker())();
}
//# sourceMappingURL=main.js.map
