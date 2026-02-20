/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package bbdd;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import javax.swing.table.DefaultTableModel;

/**
 * Clase especifica para conexión con bases de datos y realización de
 * operaciones en la misma
 *
 * @author song
 */
public class Conexion {

    public static Connection conn;

    /**
     * Método donde se establecen en los parámetros de conexión con la base de
     * datos se llamará a este método previa realización de actividades en base
     * de datos
     *
     * @return
     */
    public static Connection conectar() {

        try {
            //Identificación del driver
            Class.forName("com.mysql.jdbc.Driver");

            conn = DriverManager.getConnection("jdbc:mysql://195.35.53.72:3306/u812167471_grupo3",//servidor y bbdd
                    "u812167471_grupo3",//usuario autorizado
                    "2026-Grupo3");//contraseña
        } catch (ClassNotFoundException | SQLException ex) {
            System.getLogger(Conexion.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
        }
        return conn;

    }

    /**
     * metodo para cerrar la conexion con la base de datos
     */
    public static void cerrarConexion() {
        try {
            conn.close();
        } catch (SQLException ex) {
            System.getLogger(Conexion.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
        }
    }

    /**
     * Ejecuta una consulta SQL SELECT que devuelve un conteo y retorna ese
     * número
     *
     * @param sql
     * @return
     */
    private static int ejecutarConteo(String sql) {
        int resultado = 0;
        conectar();
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                resultado = rs.getInt(1);
            }
        } catch (SQLException ex) {
            System.getLogger(Conexion.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
        } finally {
            cerrarConexion();
        }
        return resultado;
    }

   public static class ResumenPrincipal {
    public int totalLibros;
    public int totalVolumenes;
    public int totalVentas;
}


public static ResumenPrincipal obtenerResumenPrincipal() {
    ResumenPrincipal resumen = new ResumenPrincipal();
    
    
    String sql = "SELECT " +
                 "(SELECT COUNT(*) FROM libros) AS totalLibros, " +
                 "(SELECT SUM(stock) FROM libros) AS totalVolumenes, " +
                 "((SELECT COUNT(*) FROM ventas_tienda) + (SELECT COUNT(*) FROM ventas_online)) AS totalVentas";

    conectar();
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
        if (rs.next()) {
            resumen.totalLibros = rs.getInt("totalLibros");
            resumen.totalVolumenes = rs.getInt("totalVolumenes");
            resumen.totalVentas = rs.getInt("totalVentas");
        }
    } catch (SQLException ex) {
        System.getLogger(Conexion.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
    } finally {
        cerrarConexion();
    }
    return resumen;
}

    //Aqui comienzan los metodos para generar los informes
    //____________________________________________________________________________________________________________________________
    /**
     * Genera el informe 1: Top 10 editoriales con más libros registrados.
     * Columnas: - EDITORIAL - LIBROS
     *
     * @return
     */
    public static DefaultTableModel datosInformeUno() {
        String[] col = {"EDITORIAL", "LIBROS"};
        DefaultTableModel model = new DefaultTableModel(col, 0);
        conectar();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT e.nombre, COUNT(l.idLibro) FROM editoriales e JOIN libros l ON e.idEditorial = l.idEditorial GROUP BY 1 ORDER BY 2 DESC LIMIT 10")) {
            while (rs.next()) {
                model.addRow(new Object[]{rs.getString(1), rs.getInt(2)});
            }
        } catch (SQLException ex) {
            System.getLogger(Conexion.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
        } finally {
            cerrarConexion();
        }
        return model;
    }

    /**
     * Genera el informe de facturación por vendedores activos. Columnas: -
     * VENDEDOR - FACTURACION Solo se incluyen vendedores con estado 'Activo'.
     *
     * @return
     */
    public static DefaultTableModel datosInformeDosVendedores() {
        String[] col = {"VENDEDOR", "FACTURACION"};
        DefaultTableModel model = new DefaultTableModel(col, 0);

        String sql = "SELECT v.nombre, SUM(vt.precio) "
                + "FROM vendedores v "
                + "JOIN ventas_tienda vt ON v.codVendedor = vt.codVendedor "
                + "WHERE v.idEstado = (SELECT idEstado FROM estados WHERE estado = 'Activo') "
                + "GROUP BY v.nombre";

        conectar();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                model.addRow(new Object[]{
                    rs.getString(1),
                    rs.getDouble(2)
                });
            }
        } catch (SQLException ex) {
            System.getLogger(Conexion.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
        } finally {
            cerrarConexion();
        }
        return model;
    }

    /**
     * Genera informe de facturación por plataforma online. Columnas: -
     * PLATAFORMA - FACTURACION
     *
     * @return
     */
    public static DefaultTableModel datosInformeDosPlataformas() {
        String[] col = {"PLATAFORMA", "FACTURACION"};
        DefaultTableModel model = new DefaultTableModel(col, 0);
        conectar();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT p.nombre, SUM(vo.precio) FROM plataformas p JOIN ventas_online vo ON p.idPlataforma = vo.idPlataforma GROUP BY 1")) {
            while (rs.next()) {
                model.addRow(new Object[]{rs.getString(1), rs.getDouble(2)});
            }
        } catch (SQLException ex) {
            System.getLogger(Conexion.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
        } finally {
            cerrarConexion();
        }
        return model;
    }

    /**
     * Genera informe de volúmenes por ubicación física. Usa un filtro dinámico
     * por sección (LIKE) para escoger el numero por el que empieza la seccion
     * Columnas: - UBICACION - VOLUMENES
     *
     * @return
     */
    public static DefaultTableModel datosInformeTres(String seccion) {
        String[] columnas = {"UBICACION", "VOLUMENES"};
        DefaultTableModel modelo = new DefaultTableModel(columnas, 0);

        String sql = "SELECT codUbicacion, SUM(stock) "
                + "FROM libros "
                + "WHERE codUbicacion LIKE " + seccion
                + "GROUP BY codUbicacion";

        conectar();
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                modelo.addRow(new Object[]{
                    rs.getObject(1),
                    rs.getObject(2)
                });
            }
        } catch (SQLException ex) {
            System.getLogger(Conexion.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
        } finally {
            cerrarConexion();
        }
        return modelo;
    }
    
    
    /**
     * Obtiene el total de volúmenes de una sección específica (Informe 3).
     * @param where El valor para el filtro LIKE (ej. "'1%'")
     * @return El sumatorio del stock como entero
     */
    
    public static int totalInforme3(String where) {
        int all = 0;
        // Es importante asegurar los espacios en el SQL
        String sql = "SELECT SUM(stock) FROM libros WHERE codUbicacion LIKE " + where;

        conectar();
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                all = rs.getInt(1);
            }

        } catch (SQLException ex) {
            // En Java Swing usamos JOptionPane para mostrar mensajes de error
            javax.swing.JOptionPane.showMessageDialog(null, "Error en Informe 3: " + ex.getMessage());
            java.util.logging.Logger.getLogger(Conexion.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } finally {
            cerrarConexion();
        }
        return all;
    }

    /**
     * Informe de libros por Comunidad Autónoma (CCAA). Columnas: - CCAA -
     * LIBROS
     *
     * @return DefaultTableModel
     */
    public static DefaultTableModel datosInformeCuatro() {
        String[] col = {"CCAA", "LIBROS"};
        DefaultTableModel model = new DefaultTableModel(col, 0);
        conectar();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT le.ccaa, COUNT(l.idLibro) FROM lugar_edicion le JOIN libros l ON le.idLugar = l.idLugar GROUP BY 1 ORDER BY COUNT(l.idLibro) DESC ")) {
            while (rs.next()) {
                model.addRow(new Object[]{rs.getString(1), rs.getInt(2)});
            }
        } catch (SQLException ex) {
            System.getLogger(Conexion.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
        } finally {
            cerrarConexion();
        }
        return model;
    }

    /**
     * Informe Top 5 ciudades con más libros registrados. Columnas: - CIUDAD -
     * LIBROS
     *
     * @return DefaultTableModel
     */
    public static DefaultTableModel datosInformeCinco() {
        String[] col = {"CIUDAD", "LIBROS"};
        DefaultTableModel model = new DefaultTableModel(col, 0);
        conectar();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT le.lugar, COUNT(l.idLibro) FROM lugar_edicion le JOIN libros l ON le.idLugar = l.idLugar GROUP BY 1 ORDER BY 2 DESC LIMIT 5")) {
            while (rs.next()) {
                model.addRow(new Object[]{rs.getString(1), rs.getInt(2)});
            }
        } catch (SQLException ex) {
            System.getLogger(Conexion.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
        } finally {
            cerrarConexion();
        }
        return model;
    }

    
    /**
     * Obtiene los tres libros más vendidos en tienda física.
     *
     * @return DefaultTableModel con columnas LIBROS, VOLUMENES, VENTA
     */
    
    public static DefaultTableModel obtenerTopVentasTienda() {
        String[] col = {"LIBROS", "VENTA"};
        DefaultTableModel model = new DefaultTableModel(col, 0);
        String sql = "SELECT l.titulo, l.stock, COUNT(vt.idVenta) "
                + "FROM libros l "
                + "JOIN ventas_tienda vt ON l.idLibro = vt.idLibro "
                + "GROUP BY l.idLibro "
                + "ORDER BY 3 DESC LIMIT 3";
        conectar();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                model.addRow(new Object[]{rs.getString(1), rs.getInt(2), rs.getInt(3)});
            }
        } catch (SQLException ex) {
            System.getLogger(Conexion.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
        } finally {
            cerrarConexion();
        }
        return model;
    }

    
    /**
     * Obtiene los tres libros más vendidos a través de plataformas online.
     *
     * @return DefaultTableModel con columnas LIBROS, VOLUMENES, VENTA
     */
    
    public static DefaultTableModel obtenerTopVentasOnline() {
        String[] col = {"LIBROS", "VENTA"};
        DefaultTableModel model = new DefaultTableModel(col, 0);
        String sql = "SELECT l.titulo, l.stock, COUNT(vo.idVenta) "
                + "FROM libros l "
                + "JOIN ventas_online vo ON l.idLibro = vo.idLibro "
                + "GROUP BY l.idLibro "
                + "ORDER BY 3 DESC LIMIT 3";
        conectar();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                model.addRow(new Object[]{rs.getString(1), rs.getInt(2), rs.getInt(3)});
            }
        } catch (SQLException ex) {
            System.getLogger(Conexion.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
        } finally {
            cerrarConexion();
        }
        return model;
    }

}
