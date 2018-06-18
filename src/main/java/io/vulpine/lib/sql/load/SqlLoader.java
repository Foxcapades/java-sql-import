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
package io.vulpine.lib.sql.load;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.regex.Pattern;

/**
 * <h1>SqlLoader</h1>
 *
 * <p>Provides methods for reading SQL files from the local resources structured in
 * directories organized by query type.</p>
 *
 * <p>Queries are cached internally by path so repeated loads will only incur the
 * filesystem IO cost once.</p>
 *
 * <p>Expects resource directories to appear as follows:</p>
 *
 * <pre>
 *   /{basePath}
 *    ├─ /delete
 *    │   ├─ /comments
 *    │   │   ├─ by-id.sql
 *    │   │   └─ by-user.sql
 *    │   └─ /users
 *    │       └─ by-id.sql
 *    ├─ /insert
 *    │   ├─ /comment.sql
 *    │   └─ /user.sql
 *    ├─ /select
 *    (etc...)
 * </pre>
 *
 * <p>Given the example directory above, a query to delete users by id could be
 * loaded by calling.</p>
 *
 * <pre>{@code
 * var sql = loader.delete('users.by-id');
 * }</pre>
 *
 * <p>Likewise, a query to insert a comment could be loaded by calling.</p>
 *
 * <pre>{@code
 * var sql = loader.insert('comment');
 * }</pre>
 */
public class SqlLoader
{
  private static final Pattern dotToSlash = Pattern.compile("\\.");

  private static final Pattern comments = Pattern.compile("(/^\\s*\\*[\\s\\S]+?\\*/)|(--.*)*");

  private Map < String, String > sqlMap = new HashMap <>();

  private String basePath = "/sql/";

  private String insertPath = "insert/";

  private String deletePath = "delete/";

  private String updatePath = "update/";

  private String selectPath = "select/";

  private String mergePath = "merge/";

  private String createPath = "create/";

  private String alterPath = "alter/";

  private String renamePath = "rename/";

  private String truncatePath = "truncate/";

  private String dropPath = "drop/";

  private String grantPath = "grant/";

  private String revokePath = "revoke/";

  public SqlLoader() {
  }

  public SqlLoader(String basePath) {
    this.basePath = basePath;
  }

  public String getAlterPath() {
    return alterPath;
  }

  /**
   * Overrides the default path element for <code>ALTER</code> queries.
   *
   * @param s new path component.
   *
   * @return the current SqlLoader instance.
   */
  public SqlLoader setAlterPath(String s) {
    alterPath = s.endsWith("/") ? s : s + "/";
    return this;
  }

  public String getBasePath() {
    return basePath;
  }

  /**
   * Overrides the root path element for all query types.
   *
   * @param s new path component.
   *
   * @return the current SqlLoader instance.
   */
  public SqlLoader setBasePath(String s) {
    basePath = s.endsWith("/") ? s : s + "/";
    return this;
  }

  public String getCreatePath() {
    return createPath;
  }

  /**
   * Overrides the default path element for <code>CREATE</code> queries.
   *
   * @param s new path component.
   *
   * @return the current SqlLoader instance.
   */
  public SqlLoader setCreatePath(String s) {
    createPath = s.endsWith("/") ? s : s + "/";
    return this;
  }

  public String getDeletePath() {
    return deletePath;
  }


  /**
   * Overrides the default path element for <code>DELETE</code> queries.
   *
   * @param s new path component.
   *
   * @return the current SqlLoader instance.
   */
  public SqlLoader setDeletePath(String s) {
    deletePath = s.endsWith("/") ? s : s + "/";
    return this;
  }

  public String getDropPath() {
    return dropPath;
  }


  /**
   * Overrides the default path element for <code>DROP</code> queries.
   *
   * @param s new path component.
   *
   * @return the current SqlLoader instance.
   */
  public SqlLoader setDropPath(String s) {
    dropPath = s.endsWith("/") ? s : s + "/";
    return this;
  }

  public String getGrantPath() {
    return grantPath;
  }

  /**
   * Overrides the default path element for <code>GRANT</code> queries.
   *
   * @param s new path component.
   *
   * @return the current SqlLoader instance.
   */
  public SqlLoader setGrantPath(String s) {
    grantPath = s.endsWith("/") ? s : s + "/";
    return this;
  }

  public String getInsertPath() {
    return insertPath;
  }

  /**
   * Overrides the default path element for <code>INSERT</code> queries.
   *
   * @param s new path component.
   *
   * @return the current SqlLoader instance.
   */
  public SqlLoader setInsertPath(String s) {
    insertPath = s.endsWith("/") ? s : s + "/";
    return this;
  }

  public String getMergePath() {
    return mergePath;
  }

  /**
   * Overrides the default path element for <code>MERGE</code> queries.
   *
   * @param s new path component.
   *
   * @return the current SqlLoader instance.
   */
  public SqlLoader setMergePath(String s) {
    mergePath = s.endsWith("/") ? s : s + "/";
    return this;
  }

  public String getRenamePath() {
    return renamePath;
  }

  /**
   * Overrides the default path element for <code>RENAME</code> queries.
   *
   * @param s new path component.
   *
   * @return the current SqlLoader instance.
   */
  public SqlLoader setRenamePath(String s) {
    renamePath = s.endsWith("/") ? s : s + "/";
    return this;
  }

  public String getRevokePath() {
    return revokePath;
  }

  /**
   * Overrides the default path element for <code>REVOKE</code> queries.
   *
   * @param s new path component.
   *
   * @return the current SqlLoader instance.
   */
  public SqlLoader setRevokePath(String s) {
    revokePath = s.endsWith("/") ? s : s + "/";
    return this;
  }

  public String getSelectPath() {
    return selectPath;
  }

  /**
   * Overrides the default path element for <code>SELECT</code> queries.
   *
   * @param s new path component.
   *
   * @return the current SqlLoader instance.
   */
  public SqlLoader setSelectPath(String s) {
    selectPath = s.endsWith("/") ? s : s + "/";
    return this;
  }

  public String getTruncatePath() {
    return truncatePath;
  }

  /**
   * Overrides the default path element for <code>TRUNCATE</code> queries.
   *
   * @param s new path component.
   *
   * @return the current SqlLoader instance.
   */
  public SqlLoader setTruncatePath(String s) {
    truncatePath = s.endsWith("/") ? s : s + "/";
    return this;
  }

  public String getUpdatePath() {
    return updatePath;
  }

  /**
   * Overrides the default path element for <code>UPDATE</code> queries.
   *
   * @param s new path component.
   *
   * @return the current SqlLoader instance.
   */
  public SqlLoader setUpdatePath(String s) {
    updatePath = s.endsWith("/") ? s : s + "/";
    return this;
  }

  /**
   * Attempts to load an SQL file from a resource directory structured as
   * <code>/{basePath}/{alterPath}/{input param}.sql</code>.
   *
   * Valid input paths include dot notation <code>path.to.my.file</code>, normal
   * file paths <code>path/to/my/file.sql</code>, and including or leaving off
   * the <code>.sql</code> suffix.
   *
   * @param path location of SQL file to load relative to
   *             <code>/{basePath}/{alterPath}</code>
   *
   * @return Optional SQL contents if file was found and successfully loaded.
   */
  public Optional < String > alter(String path) {
    return rawSql(alterPath + path);
  }

  /**
   * Attempts to load an SQL file from a resource directory structured as
   * <code>/{basePath}/{createPath}/{input param}.sql</code>.
   *
   * Valid input paths include dot notation <code>path.to.my.file</code>, normal
   * file paths <code>path/to/my/file.sql</code>, and including/leaving off the
   * <code>.sql</code> suffix.
   *
   * @param path location of SQL file to load relative to
   *             <code>/{basePath}/{createPath}</code>
   *
   * @return Optional SQL contents if file was found and successfully loaded.
   */
  public Optional < String > create(String path) {
    return rawSql(createPath + path);
  }

  /**
   * Attempts to load an SQL file from a resource directory structured as
   * <code>/{basePath}/{deletePath}/{input param}.sql</code>.
   *
   * Valid input paths include dot notation <code>path.to.my.file</code>, normal
   * file paths <code>path/to/my/file.sql</code>, and including or leaving off
   * the <code>.sql</code> suffix.
   *
   * @param path location of SQL file to load relative to
   *             <code>/{basePath}/{deletePath}</code>.
   *
   * @return Optional SQL contents if file was found and successfully loaded.
   */
  public Optional < String > delete(String path) {
    return rawSql(deletePath + path);
  }

  /**
   * Attempts to load an SQL file from a resource directory structured as
   * <code>/{basePath}/{dropPath}/{input param}.sql</code>.
   *
   * Valid input paths include dot notation <code>path.to.my.file</code>, normal
   * file paths <code>path/to/my/file.sql</code>, and including or leaving off
   * the <code>.sql</code> suffix.
   *
   * @param path location of SQL file to load relative to
   *             <code>/{basePath}/{dropPath}</code>.
   *
   * @return Optional SQL contents if file was found and successfully loaded.
   */
  public Optional < String > drop(String path) {
    return rawSql(dropPath + path);
  }

  /**
   * Attempts to load an SQL file from a resource directory structured as
   * <code>/{basePath}/{grantPath}/{input param}.sql</code>.
   *
   * Valid input paths include dot notation <code>path.to.my.file</code>, normal
   * file paths <code>path/to/my/file.sql</code>, and including/leaving off the
   * <code>.sql</code> suffix.
   *
   * @param path location of SQL file to load relative to
   *             <code>/{basePath}/{grantPath}</code>
   *
   * @return Optional SQL contents if file was found and successfully loaded.
   */
  public Optional < String > grant(String path) {
    return rawSql(grantPath + path);
  }

  /**
   * Attempts to load an SQL file from a resource directory structured as
   * <code>/{basePath}/{insertPath}/{input param}.sql</code>.
   *
   * Valid input paths include dot notation <code>path.to.my.file</code>, normal
   * file paths <code>path/to/my/file.sql</code>, and including or leaving off
   * the <code>.sql</code> suffix.
   *
   * @param path location of SQL file to load relative to
   *             <code>/{basePath}/{insertPath}</code>
   *
   * @return Optional SQL contents if file was found and successfully loaded.
   */
  public Optional < String > insert(String path) {
    return rawSql(insertPath + path);
  }

  /**
   * Attempts to load an SQL file from a resource directory structured as
   * <code>/{basePath}/{mergePath}/{input param}.sql</code>.
   *
   * Valid input paths include dot notation <code>path.to.my.file</code>, normal
   * file paths <code>path/to/my/file.sql</code>, and including/leaving off the
   * <code>.sql</code> suffix.
   *
   * @param path location of SQL file to load relative to
   *             <code>/{basePath}/{mergePath}</code>
   *
   * @return Optional SQL contents if file was found and successfully loaded.
   */
  public Optional < String > merge(String path) {
    return rawSql(mergePath + path);
  }

  public Optional < String > rawSql(final String path) {
    if (sqlMap.containsKey(path)) {
      return Optional.of(sqlMap.get(path));
    }

    var parsed = dotToSlash.matcher(path).replaceAll("/");
    var in     = getClass().getResourceAsStream(basePath + parsed + ".sql");

    if (in == null) return Optional.empty();

    var sql    = new Scanner(in);
    var build  = new StringBuilder();

    do {
      var s = sql.skip(comments).nextLine();
      if (!s.isEmpty()) {
        build.append(s).append(System.getProperty("line.separator", "\n"));
      }
    } while (sql.hasNextLine());

    sqlMap.put(path, build.toString());

    return Optional.of(build.toString());
  }

  /**
   * Attempts to load an SQL file from a resource directory structured as
   * <code>/{basePath}/{renamePath}/{input param}.sql</code>.
   *
   * Valid input paths include dot notation <code>path.to.my.file</code>, normal
   * file paths <code>path/to/my/file.sql</code>, and including/leaving off the
   * <code>.sql</code> suffix.
   *
   * @param path location of SQL file to load relative to
   *             <code>/{basePath}/{renamePath}</code>
   *
   * @return Optional SQL contents if file was found and successfully loaded.
   */
  public Optional < String > rename(String path) {
    return rawSql(renamePath + path);
  }

  /**
   * Attempts to load an SQL file from a resource directory structured as
   * <code>/{basePath}/{revokePath}/{input param}.sql</code>.
   *
   * Valid input paths include dot notation <code>path.to.my.file</code>, normal
   * file paths <code>path/to/my/file.sql</code>, and including/leaving off the
   * <code>.sql</code> suffix.
   *
   * @param path location of SQL file to load relative to
   *             <code>/{basePath}/{revokePath}</code>
   *
   * @return Optional SQL contents if file was found and successfully loaded.
   */
  public Optional < String > revoke(String path) {
    return rawSql(revokePath + path);
  }

  /**
   * Attempts to load an SQL file from a resource directory structured as
   * <code>/{basePath}/{selectPath}/{input param}.sql</code>.
   *
   * Valid input paths include dot notation <code>path.to.my.file</code>, normal
   * file paths <code>path/to/my/file.sql</code>, and including/leaving off the
   * <code>.sql</code> suffix.
   *
   * @param path location of SQL file to load relative to
   *             <code>/{basePath}/{selectPath}</code>
   *
   * @return Optional SQL contents if file was found and successfully loaded.
   */
  public Optional < String > select(String path) {
    return rawSql(selectPath + path);
  }

  /**
   * Attempts to load an SQL file from a resource directory structured as
   * <code>/{basePath}/{truncatePath}/{input param}.sql</code>.
   *
   * Valid input paths include dot notation <code>path.to.my.file</code>, normal
   * file paths <code>path/to/my/file.sql</code>, and including/leaving off the
   * <code>.sql</code> suffix.
   *
   * @param path location of SQL file to load relative to
   *             <code>/{basePath}/{truncatePath}</code>
   *
   * @return Optional SQL contents if file was found and successfully loaded.
   */
  public Optional < String > truncate(String path) {
    return rawSql(truncatePath + path);
  }

  /**
   * Attempts to load an SQL file from a resource directory structured as
   * <code>/{basePath}/{updatePath}/{input param}.sql</code>.
   *
   * Valid input paths include dot notation <code>path.to.my.file</code>, normal
   * file paths <code>path/to/my/file.sql</code>, and including/leaving off the
   * <code>.sql</code> suffix.
   *
   * @param path location of SQL file to load relative to
   *             <code>/{basePath}/{updatePath}</code>
   *
   * @return Optional SQL contents if file was found and successfully loaded.
   */
  public Optional < String > udpate(String path) {
    return rawSql(updatePath + path);
  }
}
