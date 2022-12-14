import logo from './logo.svg';
import './App.css';
import io from "socket.io-client"
import * as d3 from "d3";

function App() {
  console.log(process.env)
  const socket = io("http://localhost:9092/", { transports: ["websocket"] })

  return (
    <div className="App">
      <header className="App-header">
        <img src={logo} className="App-logo" alt="logo" />
        <p>
          Edit <code>src/App.js</code> and save to reload.
        </p>
        <a
          className="App-link"
          href="https://reactjs.org"
          target="_blank"
          rel="noopener noreferrer"
        >
          Learn React
        </a>
      </header>
    </div>
  );
}

export default App;
