const { DataTypes } = require("sequelize");
const { sequelize1 } = require("../BD-RFAS8/conexiondb");

const USUARIO = require("./usuario.model");
const PERMISO = require("./permiso.model");

const UsuarioPermiso = sequelize1.define("usuarioPermiso", {

});

USUARIO.belongsToMany(PERMISO, { through: UsuarioPermiso });
PERMISO.belongsToMany(USUARIO, { through: UsuarioPermiso });

module.exports = UsuarioPermiso;
