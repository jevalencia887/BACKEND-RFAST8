const { DataTypes } = require("sequelize");
const { sequelize1 } = require("../BD-RFAS8/conexiondb");
const USUARIO = require("./usuario.model");

const PERFIL = sequelize1.define("perfil",
  {
    id_perfil: {
      type: DataTypes.INTEGER,
      primaryKey: true,
      autoIncrement: true
    },
    nombre: {
      type: DataTypes.STRING(100)
    },
  },
  
);

USUARIO.belongsTo(PERFIL,{
  foreignKey: "id_perfil"
});

module.exports = PERFIL;