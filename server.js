const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const bodyParser = require('body-parser');
const cors = require('cors');
const { Server } = require('socket.io');
const kafka = require('kafka-node');

const app = express();
const port = 8000;

app.use(cors());
app.use(bodyParser.json());

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: 'http://localhost:3000',
    methods: ['GET', 'POST'],
  },
});

const kafkaClient = new kafka.KafkaClient({ kafkaHost: 'localhost:9092' });
const producer = new kafka.Producer(kafkaClient);

const clients = new Set();

app.post('/api/receive-data', (req, res) => {
    const receivedData = req.body;

    const jsonData = JSON.stringify(receivedData);
    clients.forEach(client => {
        client.send(jsonData);
    });

    res.status(200).send('Data received successfully');
});

app.post('/send-request', (req, res) => {
    const selectedCity = req.body.city;

    const payload = [
        {
            topic: 'topic1',
            messages: [selectedCity],
        },
    ];

    producer.send(payload, (err, data) => {
        if (err) {
            console.error('Greska pri slanju poruke na Kafka:', err);
            res.status(500).json({ message: `Greska pri slanju poruke u topic1: ${err.message}` });
        } else {
            console.log('Poruka uspesno poslata na Kafka:', data);

            res.status(200).json({ message: `Grad ${selectedCity} uspesno poslat u topic1 i obradjen!` });
        }
    });
});

io.on('connection', (socket) => {
    console.log('New WebSocket connection');

    clients.add(socket);

    socket.on('disconnect', () => {
        console.log('WebSocket connection closed');
        clients.delete(socket);
    });
});

server.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
});