module mss.dbmigration {

   exports de.mss.db.migration;

   requires transitive mss.utils;
   requires transitive mss.configtools;
   requires transitive java.sql;
   requires transitive org.flywaydb.core;
}
