const { Router } = require("express");
const InfanciaController = require("../controllers/curso-vida.controller");
const usuariosController = require("../controllers/usuario.controller");
const { validarUsuario } = require("../auxiliar/usuario.validador");



const router = Router();


//Rutas para Cursos de Vida
router.get("/curso-vida/:CODIGO/:edadInicial/:edadFinal", InfanciaController.cursoVida);
router.post("/curso-vida/buscar/:CODIGO/:edadInicial/:edadFinal", InfanciaController.buscar);

//Rutas para perfiles
router.get("/perfiles", usuariosController.listaPerfiles);
router.post("/perfiles", usuariosController.crearPerfil);

//Rutas de Usuarios
router.get("/usuario", usuariosController.listarUsuarios);
router.post("/usuario", validarUsuario, usuariosController.crearUsuario);


module.exports = router;
