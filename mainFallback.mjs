// POST /payments
// {
//     "correlationId": "4a7901b8-7d26-4d9d-aa19-4dc1c7cf60b3",
//     "amount": 19.90,
//     "requestedAt" : "2025-07-15T12:34:56.000Z"
// }

// crie um servidor http para receber essa mensagem e mostrar um log com http em js

import { createServer } from 'http';

const server = createServer((req, res) => {
    if (req.method === 'POST' && req.url === '/payments') {
        let body = '';
        req.on('data', chunk => {
            body += chunk.toString();
        });
        req.on('end', () => {
            console.log(`Received payment: ${body}`);
            res.writeHead(204);
            res.end();
        });
    } else {
        res.writeHead(404);
        res.end();
    }
});

server.listen(4001, () => {
    console.log('Server is listening on port 4001');
});