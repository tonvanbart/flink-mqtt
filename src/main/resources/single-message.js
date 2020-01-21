class SingleMessage extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      error: null,
      connected: false,
      message: null,
      host: "broker.hivemq.com",
      port: 8000,
      reconnect: 2000
    };
    // this.mqtt = null;
    this.reconnect = 2000;
    this.host = "broker.hivemq.com";
    this.port = 8000;
    console.log("SingleMessage initialized");
    this.onConnect = this.onConnect.bind(this);
  }

  onConnect() {
    console.log("Connected to broker");
    console.log("this.mqtt=" + this.mqtt);
    this.state.message = "Connected to broker";
    this.mqtt.subscribe("github.tonvanbart/wikiedits");
    this.state.connected = true;
  }

  onFailure() {
      this.state.message = "Connection to broker failed!"
      console.log("Connection to broker failed");
      this.state.connected = false;
  }

  messageArrived(msg) {
      console.log("Topic: " + msg.destinationName + ", message: " + msg.payloadString)
      this.state.message = msg.payloadString;
  }

  componentDidMount() {
    console.log("componentDidMount()");

    this.mqtt = new Paho.MQTT.Client(this.state.host, this.state.port, "clientjs");
    console.log("created instance");
    var options = {
        timeout: 3,
        onSuccess: this.onConnect,
        onFailure: this.onFailure,
    };
    console.log("options=" + options);
    this.mqtt.onMessageArrived = this.messageArrived;
    this.mqtt.connect(options);
  }

  render() {
    return (
      <div>
      <span id="host">{this.state.host}:{this.state.port}</span>
      <span id="message">{this.state.message}</span>
      </div>
    );
  }
}

ReactDOM.render(
  <SingleMessage />,
  document.getElementById('single-message-container')
);