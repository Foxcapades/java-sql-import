/**
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

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Pattern;

@SuppressWarnings( "unused" )
public class SqlImport
{
  private static final Pattern dotToSlash = Pattern.compile("\\.");
  private static final Pattern comments   = Pattern.compile("(/^\\s*\\*[\\s\\S]+?\\*/)|(--.*)*");

  private static String basePath = "/sql/";
  private static String insertPath = "insert/";
  private static String deletePath = "delete/";
  private static String updatePath = "update/";
  private static String selectPath = "select/";
  private static String mergePath = "merge/";
  private static String createPath = "create/";
  private static String alterPath = "alter/";
  private static String renamePath = "rename/";
  private static String truncatePath = "truncate/";
  private static String dropPath = "drop/";
  private static String grantPath = "grant/";
  private static String revokePath = "revoke/";

  private static Map< String, String > sqlMap = new HashMap<>();

  public static String rawSql ( final String path )
  {
    final InputStream in;
    final Scanner     sql;
    final String      parsedPath;
    final StringBuilder build;

    if (sqlMap.containsKey(path)) return sqlMap.get(path);

    parsedPath = dotToSlash.matcher(path).replaceAll("/");

    in = io.vulpine.sql.resource.SqlImport.class.getResourceAsStream(basePath + parsedPath + ".sql");

    sql = new Scanner(in);
    build = new StringBuilder();

    do {
      final String s = sql.skip(comments).nextLine();
      if (!s.isEmpty()) build.append(s).append(System.lineSeparator());
    } while (sql.hasNextLine());

    sqlMap.put(path, build.toString());

    return build.toString();
  }

  public static String insert ( final String path )
  {
    return rawSql(insertPath + path);
  }

  public static String delete ( final String path )
  {
    return rawSql(deletePath + path);
  }

  public static String udpate ( final String path )
  {
    return rawSql(updatePath + path);
  }

  public static String select ( final String path )
  {
    return rawSql(selectPath + path);
  }

  public static String truncate ( final String path )
  {
    return rawSql(truncatePath + path);
  }

  public static String alter ( final String path )
  {
    return rawSql(alterPath + path);
  }

  public static String merge ( final String path )
  {
    return rawSql(mergePath + path);
  }

  public static String create ( final String path )
  {
    return rawSql(createPath + path);
  }

  public static String drop ( final String path )
  {
    return rawSql(dropPath + path);
  }

  public static String rename ( final String path )
  {
    return rawSql(renamePath + path);
  }

  public static String grant ( final String path )
  {
    return rawSql(grantPath + path);
  }

  public static String revoke ( final String path )
  {
    return rawSql(revokePath + path);
  }

  public static String getBasePath ()
  {
    return basePath;
  }

  public static String getInsertPath ()
  {
    return insertPath;
  }

  public static String getDeletePath ()
  {
    return deletePath;
  }

  public static String getUpdatePath ()
  {
    return updatePath;
  }

  public static String getSelectPath ()
  {
    return selectPath;
  }

  public static String getMergePath ()
  {
    return mergePath;
  }

  public static String getCreatePath ()
  {
    return createPath;
  }

  public static String getAlterPath ()
  {
    return alterPath;
  }

  public static String getRenamePath ()
  {
    return renamePath;
  }

  public static String getTruncatePath ()
  {
    return truncatePath;
  }

  public static String getDropPath ()
  {
    return dropPath;
  }

  public static String getGrantPath ()
  {
    return grantPath;
  }

  public static String getRevokePath ()
  {
    return revokePath;
  }

  public static void setBasePath ( String s )
  {
    basePath = s.endsWith("/") ? s : s + "/";
  }

  public static void setInsertPath ( String s )
  {
    insertPath = s.endsWith("/") ? s : s + "/";
  }

  public static void setDeletePath ( String s )
  {
    deletePath = s.endsWith("/") ? s : s + "/";
  }

  public static void setUpdatePath ( String s )
  {
    updatePath = s.endsWith("/") ? s : s + "/";
  }

  public static void setSelectPath ( String s )
  {
    selectPath = s.endsWith("/") ? s : s + "/";
  }

  public static void setMergePath ( String s )
  {
    mergePath = s.endsWith("/") ? s : s + "/";
  }

  public static void setCreatePath ( String s )
  {
    createPath = s.endsWith("/") ? s : s + "/";
  }

  public static void setAlterPath ( String s )
  {
    alterPath = s.endsWith("/") ? s : s + "/";
  }

  public static void setRenamePath ( String s )
  {
    renamePath = s.endsWith("/") ? s : s + "/";
  }

  public static void setTruncatePath ( String s )
  {
    truncatePath = s.endsWith("/") ? s : s + "/";
  }

  public static void setDropPath ( String s )
  {
    dropPath = s.endsWith("/") ? s : s + "/";
  }

  public static void setGrantPath ( String s )
  {
    grantPath = s.endsWith("/") ? s : s + "/";
  }

  public static void setRevokePath ( String s )
  {
    revokePath = s.endsWith("/") ? s : s + "/";
  }
}
