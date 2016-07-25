package io.vulpine.sql.resource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public abstract class AbstractRepository
{
  protected final DataSource dataSource;

  public AbstractRepository ( final DataSource dataSource )
  {
    this.dataSource = dataSource;
  }

  protected static void close ( final AutoCloseable... q ) throws Exception
  {
    for ( final AutoCloseable a : q ) {

      if (a instanceof ResultSet) {

        final ResultSet  r = (ResultSet) a;
        final Statement  s = r.getStatement();
        final Connection c = s.getConnection();

        if (!r.isClosed())
          r.close();

        if (!s.isClosed()) {
          if (s instanceof PreparedStatement)
            ((PreparedStatement) s).clearParameters();
          s.close();
        }

        if (!c.isClosed())
          c.close();

      } else if (a instanceof PreparedStatement) {

        final PreparedStatement p = (PreparedStatement) a;
        final Connection        c = p.getConnection();

        if (!p.isClosed()) {
          p.clearParameters();
          p.close();
        }

        if (!c.isClosed())
          c.close();

      } else if (a instanceof Statement) {

        final Statement  s = (Statement) a;
        final Connection c = s.getConnection();

        if (!s.isClosed())
          s.close();

        if (!c.isClosed())
          c.close();

      } else {

        a.close();

      }
    }
  }
}
