package de.mss.db.migration;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.callback.Callback;
import org.flywaydb.core.api.configuration.FluentConfiguration;

import de.mss.configtools.ConfigFile;
import de.mss.utils.exception.MssException;

public class MssDbMigration {

   private ConfigFile                   cfgFile       = null;
   private String                       dbName        = null;
   private de.mss.utils.exception.Error error         = null;

   private final List<String>           fileLocations = new ArrayList<>();
   private final List<Callback>         callbacks     = new ArrayList<>();

   private Connection                   dbCon         = null;

   public MssDbMigration(ConfigFile cfg, String name, de.mss.utils.exception.Error e) {
      this.cfgFile = cfg;
      this.dbName = name;
      this.error = e;
   }


   public void addCallback(Callback cbfn) {
      this.callbacks.add(cbfn);
   }


   private void addDefaultLocations(boolean fromFilesystem) {
      addFileLocation("db/design", fromFilesystem);
      addFileLocation("db/scripts", fromFilesystem);
      addFileLocation("db/values", fromFilesystem);
      addFileLocation("db/trigger", fromFilesystem);
   }


   public void addFileLocation(String fl, boolean fromFilesystem) {
      final String loc = getLocationPrefix(fromFilesystem) + ":" + fl;
      if (!this.fileLocations.contains(loc)) {
         this.fileLocations.add(loc);
      }
   }


   private void checkAndLoadDbDriver() throws MssException {
      try {
         final Driver instance = (Driver)Class
               .forName(getCfgDriver())
               .getDeclaredConstructor()
               .newInstance();
         DriverManager.registerDriver(instance);
      }
      catch (final Exception e) {
         throw new MssException(this.error, e);
      }
   }


   @SuppressWarnings("resource")
   private boolean checkFlywayTables() throws MssException {
      getConnection();
      try (
           Statement stmt = this.dbCon.createStatement();
           ResultSet res = stmt.executeQuery("select * from flyway_schema_history");
      ) {

         return true;
      }
      catch (final Exception e) {
         throw new MssException(this.error, e);
      }
   }


   private String getCfgDriver() {
      return this.cfgFile.getValue(this.dbName + ".driver", (String)null);
   }


   private String getCfgPasswd() {
      return this.cfgFile.getValue(this.dbName + ".passwd", (String)null);
   }


   private String getCfgUrl() {
      return this.cfgFile.getValue(this.dbName + ".url", (String)null);
   }


   private String getCfgUser() {
      return this.cfgFile.getValue(this.dbName + ".user", (String)null);
   }


   private Connection getConnection() throws MssException {
      if (this.dbCon == null) {
         checkAndLoadDbDriver();
         try {
            this.dbCon = DriverManager
                  .getConnection(
                        getCfgUrl(),
                        getCfgUser(),
                        getCfgPasswd());
         }
         catch (final Exception e) {
            throw new MssException(this.error, e);
         }
      }
      return this.dbCon;
   }


   private String getLocationPrefix(boolean fromFilesystem) {
      return fromFilesystem ? "filesystem" : "classpath";
   }


   @SuppressWarnings("resource")
   public void migrateDb(boolean fromFilesystem) throws MssException {
      addDefaultLocations(fromFilesystem);

      getConnection();

      try {
         final FluentConfiguration flyway = Flyway
               .configure()
               .dataSource(
                     getCfgUrl(),
                     getCfgUser(),
                     getCfgPasswd())
               .locations(this.fileLocations.toArray(new String[this.fileLocations.size()]))
               .callbacks(this.callbacks.toArray(new Callback[this.callbacks.size()]))
               .encoding("utf-8");

         flyway.load().migrate();
      }
      catch (final Exception e) {
         throw new MssException(this.error, e);
      }
   }
}

