const { DataTypes } = require("sequelize");
const { sequelize1 } = require("../BD-RFAS8/conexiondb");


 const USUARIO = sequelize1.define("usuario", {
  
  nombres: {
    type: DataTypes.STRING,
    require: true
  },

  apellidos: {
    type: DataTypes.STRING,
    require: true
  },

  password: {
    type: DataTypes.STRING,
    require: true
  },
  cedula: {
    type: DataTypes.INTEGER,
    require: true
  },
  email: {
    type: DataTypes.STRING,
    require: true
  },

  id_perfil: {
    type: DataTypes.INTEGER,
    require: true
  }
  
});


module.exports = USUARIO;