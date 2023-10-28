const { Router } = require("express");
const cursoVidaController = require("../controllers/curso-vida.controller");
const DashboardServiciosController = require("../controllers/dashboard-servicios.controller")
const usuariosController = require("../controllers/usuario.controller");
const { validarUsuario } = require("../auxiliar/usuario.validador");
const   loginController  = require("../controllers/autenticacion.controller");
const { validarJWT } = require("../middlewares/jwt-validar");



const router = Router();


//Rutas para Cursos de Vida
router.get("/curso-vida/:CODIGO/:edadInicial/:edadFinal", validarJWT, cursoVidaController.cursoVida);
router.post("/curso-vida/buscar/:CODIGO/:edadInicial/:edadFinal", validarJWT, cursoVidaController.buscar);
router.get("/dashboard-servicios", validarJWT, DashboardServiciosController.servicios);

//Rutas para perfiles
router.get("/perfiles", validarJWT, usuariosController.listaPerfiles);
router.post("/perfiles", validarJWT, usuariosController.crearPerfil);

//Rutas de Usuarios
router.get("/usuario", validarJWT,  usuariosController.listarUsuarios);
router.post("/usuario", [validarUsuario, validarJWT,],  usuariosController.crearUsuario);
router.patch("/usuario/:id",  validarJWT,  usuariosController.editarUsuario);
/* router.patch("/usuario/:id",  validarJWT,  usuariosController.editarUsuario); */
router.put("/usuario/:id/estado",  validarJWT,  usuariosController.actualizarEstadoUsuario);
router.post("/login", loginController.login);
router.get("/validar-token/:id", validarJWT, loginController.validarToken);
router.get("/exportar-excel0/:CODIGO/:edadInicial/:edadFinal", /* validarJWT, */ cursoVidaController.cursoVidaExcel0);
router.get("/exportar-excel1/:CODIGO/:edadInicial/:edadFinal", /* validarJWT, */ cursoVidaController.cursoVidaExcel1);
router.get("/exportar-excel2/:CODIGO/:edadInicial/:edadFinal", /* validarJWT, */ cursoVidaController.cursoVidaExcel2);
router.get("/exportar-excel3/:CODIGO/:edadInicial/:edadFinal", /* validarJWT, */ cursoVidaController.cursoVidaExcel3);
router.get("/exportar-excel4/:CODIGO/:edadInicial/:edadFinal", /* validarJWT, */ cursoVidaController.cursoVidaExcel4);
router.get("/exportar-excel5/:CODIGO/:edadInicial/:edadFinal", /* validarJWT, */ cursoVidaController.cursoVidaExcel5);
router.get("/listar-permisos/", validarJWT, usuariosController.listaPermisos );



module.exports = router;
