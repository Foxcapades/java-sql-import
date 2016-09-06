/*
 * Copyright 2016 Elizabeth Harper
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.vulpine.sql.resource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public abstract class AbstractRepository
{
  protected final DataSource dataSource;

  public AbstractRepository( final DataSource dataSource )
  {
    this.dataSource = dataSource;
  }

  protected static void close( final Object... q ) throws Exception
  {
    for ( final Object a : q ) {

      if (a instanceof ResultSet) {

        final ResultSet  r = (ResultSet) a;
        final Statement  s = r.getStatement();
        final Connection c = s.getConnection();

        if (!r.isClosed()) {
          r.close();
        }

        if (!s.isClosed()) {
          if (s instanceof PreparedStatement) {
            ((PreparedStatement) s).clearParameters();
          }
          s.close();
        }

        if (!c.isClosed()) {
          c.close();
        }

      } else if (a instanceof PreparedStatement) {

        final PreparedStatement p = (PreparedStatement) a;
        final Connection        c = p.getConnection();

        if (!p.isClosed()) {
          p.clearParameters();
          p.close();
        }

        if (!c.isClosed()) {
          c.close();
        }

      } else if (a instanceof Statement) {

        final Statement  s = (Statement) a;
        final Connection c = s.getConnection();

        if (!s.isClosed()) {
          s.close();
        }

        if (!c.isClosed()) {
          c.close();
        }

      }
    }
  }
}
