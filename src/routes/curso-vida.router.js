const { Router } = require("express");
const InfanciaController = require("../controllers/curso-vida.controller");
const usuariosController = require("../controllers/usuario.controller");
const { validarUsuario } = require("../auxiliar/usuario.validador");
const   loginController  = require("../controllers/autenticacion.controller");
const { validarJWT } = require("../middlewares/jwt-validar");



const router = Router();


//Rutas para Cursos de Vida
router.get("/curso-vida/:CODIGO/:edadInicial/:edadFinal", InfanciaController.cursoVida);
router.post("/curso-vida/buscar/:CODIGO/:edadInicial/:edadFinal", InfanciaController.buscar);

//Rutas para perfiles
router.get("/perfiles", validarJWT, usuariosController.listaPerfiles);
router.post("/perfiles", validarJWT, usuariosController.crearPerfil);

//Rutas de Usuarios
router.get("/usuario", validarJWT,  usuariosController.listarUsuarios);
router.post("/usuario", [validarUsuario, validarJWT],  usuariosController.crearUsuario);
router.post("/login", loginController.login);


module.exports = router;
