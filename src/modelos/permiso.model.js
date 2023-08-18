const { DataTypes } = require("sequelize");
const { sequelize1 } = require("../BD-RFAS8/conexiondb");


    const PERMISO = sequelize1.define("permiso", {
    
    path: {
        type: DataTypes.STRING,
        require: true
    },

    title: {
        type: DataTypes.STRING,
        require: true
    },

    rtlTitle: {
        type: DataTypes.STRING,
        require: true
    },
    icon: {
        type: DataTypes.STRING,
        require: true
    },

    class: {
        type: DataTypes.STRING,
    },
    
},
{
    timestamps: false
} );


module.exports = PERMISO;