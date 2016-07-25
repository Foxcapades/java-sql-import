package io.vulpine.sql.resource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ImportPreparedStatement
{
  public static PreparedStatement getPreparedStatement(final Connection con, final String path ) throws SQLException
  {
    final String sql = SqlImport.rawSql(path);

    return con.prepareStatement(sql);
  }

  public static PreparedStatement select( final Connection c, final String p ) throws SQLException
  {
    return c.prepareStatement(SqlImport.select(p));
  }

  public static PreparedStatement insert( final Connection c, final String p ) throws SQLException
  {
    return c.prepareStatement(SqlImport.insert(p));
  }
}
