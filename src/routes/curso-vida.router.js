const { Router } = require("express");
const InfanciaController = require("../controllers/curso-vida.controller");



const router = Router();


//Rutas para Cursos de Vida
router.get("/curso-vida/:CODIGO/:edadInicial/:edadFinal", InfanciaController.cursoVida);
router.post("/curso-vida/buscar/:CODIGO/:edadInicial/:edadFinal", InfanciaController.buscar);


module.exports = router;
