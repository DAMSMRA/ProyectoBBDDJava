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
     * Ejecuta una consulta SQL SELECT que devuelve un conteo y retorna ese número
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

    
    /**
     * Obtiene el número total de libros registrados en la base de datos.
     * Usa el método genérico ejecutarConteo para reutilizar lógica.
     * @return
     */
    
    public static int obtenerTotalLibros() {
        return ejecutarConteo("SELECT COUNT(*) FROM libros");
    }

    
    /**
     * Obtiene el número total de volúmenes físicos en almacén. Suma el stock de
     * todos los libros.
     * @return t
     */
    
    public static int obtenerTotalVolumenes() {
        int resultado = 0;
        conectar();
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("SELECT SUM(stock) FROM libros")) {
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

    
    /**
     * Obtiene el total global de ventas sumando: - ventas en tienda - ventas
     * online
     * @return
     */
    
    public static int obtenerTotalVentasGlobal() {
        return ejecutarConteo("SELECT (SELECT COUNT(*) FROM ventas_tienda) + (SELECT COUNT(*) FROM ventas_online)");
    }
    
    
    //Aqui comienzan los metodos para generar los informes
    //____________________________________________________________________________________________________________________________

    
    /**
     * Genera el informe 1: Top 10 editoriales con más libros registrados.
     * Columnas: - EDITORIAL - LIBROS
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
     * Genera el informe de facturación por vendedores activos.
     * Columnas: - VENDEDOR - FACTURACION
     * Solo se incluyen vendedores con estado 'Activo'.
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
     * Genera informe de facturación por plataforma online.
     * Columnas: - PLATAFORMA - FACTURACION
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
     * Genera informe de volúmenes por ubicación física.
     * Usa un filtro dinámico por sección (LIKE) para escoger el numero por el
     * que empieza la seccion
     * Columnas: - UBICACION - VOLUMENES
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
     * Informe de libros por Comunidad Autónoma (CCAA).
     * Columnas: - CCAA - LIBROS
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
     * Informe Top 5 ciudades con más libros registrados.
     * Columnas: - CIUDAD - LIBROS
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

}
