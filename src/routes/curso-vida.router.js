const { Router } = require("express");
const cursoVidaController = require("../controllers/curso-vida.controller");
const DashboardServiciosController = require("../controllers/dashboard-servicios.controller")
const usuariosController = require("../controllers/usuario.controller");
const { validarUsuario } = require("../auxiliar/usuario.validador");
const   loginController  = require("../controllers/autenticacion.controller");
const { validarJWT } = require("../middlewares/jwt-validar");



const router = Router();


//Rutas para Cursos de Vida
router.get("/curso-vida/:CODIGO/:edadInicial/:edadFinal", cursoVidaController.cursoVida);
router.post("/curso-vida/buscar/:CODIGO/:edadInicial/:edadFinal", cursoVidaController.buscar);
router.get("/dashboard-servicios", DashboardServiciosController.servicios);

//Rutas para perfiles
router.get("/perfiles", /* validarJWT, */ usuariosController.listaPerfiles);
router.post("/perfiles", /* validarJWT, */ usuariosController.crearPerfil);

//Rutas de Usuarios
router.get("/usuario", /* validarJWT, */  usuariosController.listarUsuarios);
router.post("/usuario", [validarUsuario,/*  validarJWT */],  usuariosController.crearUsuario);
router.patch("/usuario/:id", /*  validarJWT */  usuariosController.editarUsuario);
router.post("/login", loginController.login);
router.get("/validar-token", validarJWT, loginController.validarToken);
router.get("/exportar-excel/:CODIGO/:edadInicial/:edadFinal", cursoVidaController.cursoVidaExcel);


module.exports = router;
