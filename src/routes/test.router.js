const { Router } = require("express");
const primeraInfanciaController = require("../controllers/primera-infancia.controller");
const InfanciaController = require("../controllers/infancia.controller");
const  adolecenciaController  = require("../controllers/adolecencia.controller");
const  juventudController  = require("../controllers/juventud.controller");
const  adultezController = require("../controllers/adultez.contorller");
const  vejezController = require("../controllers/vejez.controlller");


const router = Router();


//Rutas para Cursos de Vida
router.get("/primera-infancia", primeraInfanciaController.primeraInfancia);
router.get("/infancia", InfanciaController.infancia);
router.get("/adolecencia", adolecenciaController.adolecencia);
router.get("/juventud", juventudController.juventud);
router.get("/adultez", adultezController.adultez);
router.get("/vejez", vejezController.vejez);

module.exports = router;
