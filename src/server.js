require("dotenv").config();
const express = require("express");
const cors = require("cors");
const morgan = require("morgan");

class Server {
    constructor() {
        this.app = express();
        this.port = process.env.PORT;
        this.middlewares();
        this.rutas();
    }

    middlewares() {
        this.app.use(cors());
        //this.app.use(express.json());
        this.app.use(express.json({ extende: true, parameterLimit: 10000000000000 , limit: '100000mb'}));
        this.app.use(morgan("dev"));
        //this.validarHeadres();
        this.validarMetodos();
        this.politicaSeguridad();
    }

    async validarHeadres() {
        this.app.use(function (req, res, next) {
            if (
            req.headers["x-frame-options"] != 'SAMEORIGIN' ||
            req.headers["x-xss-protection"] != '1; mode=block' ||
            req.headers["x-content-type-options"] != 'nosniff' ||
            req.headers["content-type"] != 'application/json'
            ) {
            res.status(400).send({
                messagge: 'Headers no enviadas o los valores no son validos'
            });
            } else {
            next();
            }
        });
    }

    async politicaSeguridad() {
        this.app.use(function (req, res, next) {
            res.setHeader(
            'Content-Security-Policy', "default-src 'self'; script-src 'self'; style-src 'self'; font-src 'self'; img-src 'self'; frame-src 'self'"
            );
            next();
        });
    }

    async validarMetodos() {
        this.app.use(function (req, res, next) {
            if (!(["GET", "POST", "PUT", "PATCH"].indexOf(req.method) > -1)) {
            res.sendStatus(405);
            } else {
            next();
            }
        });
    }

    rutas(){
        this.app.use(require('./routes/test.router'));
    }
        
    listen() {
        this.app.listen(this.port, () => {
        console.log("Servidor corriendo en puerto", this.port);
        });
    }
}

module.exports = Server;
