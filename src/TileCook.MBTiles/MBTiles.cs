using System;
using System.Reflection;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TileCook;
using TileProj;
using Microsoft.Data.Sqlite;
using System.Security.Cryptography;

namespace TileCook.MBTiles
{
    public class MBTiles : IWritableTileStore
    {
        private TileInfo Info;
        private string ConnectionString;
        public string FilePath { get; private set; }
        
        public MBTiles(string resource)
        {
            FilePath = resource;
            Info = new TileInfo();
            Info.Scheme = TileScheme.Tms;
            
            // Get MBTiles schema
            string schema;
            var assembly = this.GetType().GetTypeInfo().Assembly;
            using (var stream = assembly.GetManifestResourceStream("TileCook.MBTiles.schema.sql"))
            {
                using (var reader = new StreamReader(stream))
                {
                    schema = reader.ReadToEnd();
                }
            }
            
            // connect or create db
            var builder = new SqliteConnectionStringBuilder();
            builder.DataSource = FilePath;
            ConnectionString = builder.ToString();
            using (var connection = new SqliteConnection(ConnectionString))
            {
                connection.Open();
                
                // execute schema
                var schemaCommand = connection.CreateCommand();
                schemaCommand.CommandText = schema;
                schemaCommand.ExecuteNonQuery();
                
                // get TileInfo
                var metadataCommand = connection.CreateCommand();
                metadataCommand.CommandText = "SELECT name, value FROM metadata";
                using (var reader = metadataCommand.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string key = reader.GetString(0);
                        switch (key.ToLower())
                        {
                            case "name":
                                Info.Name = reader.GetString(1);
                                break;
                            case "description":
                                Info.Description = reader.GetString(1);
                                break;
                            case "format":
                                Info.Format = (TileFormat) Enum.Parse(typeof(TileFormat), reader.GetString(1), true); 
                                break;
                            case "minzoom":
                                Info.MinZoom = Int32.Parse(reader.GetString(1));
                                break;
                            case "maxzoom":
                                Info.MaxZoom = Int32.Parse(reader.GetString(1));
                                break;
                            case "bounds":
                                Info.Bounds = reader.GetString(1).Split(',').Select(x => double.Parse(x)).ToArray();
                                break;
                            case "center":
                                Info.Center = reader.GetString(1).Split(',').Select(x => double.Parse(x)).ToArray();
                                break;
                            case "json":
                            {
                                string json = reader.GetString(1);
                                // parse json
                                break;
                            }
                        }
                    }
                }
            }
        }
        
        
        public TileInfo GetTileInfo()
        {
            return Info.DeepClone();
        }
        
        public void SetTileInfo(TileInfo tileinfo)
        {
            if (!tileinfo.IsValid())
            {
                throw new ArgumentException("TileInfo is invalid", "tileinfo");
            }
            if (tileinfo.Scheme != TileScheme.Tms)
            {
                throw new ArgumentException("Scheme must be Tms", "tileinfo");
            }            
            Info = tileinfo.DeepClone();

            // write tileinfo to metadata table
            using (var connection = new SqliteConnection(ConnectionString))
            {
                connection.Open();
                var insertCommand = connection.CreateCommand();
                insertCommand.CommandText = "REPLACE INTO metadata (name, value) VALUES (@name, @value)";
                
                // name
                insertCommand.Parameters.AddWithValue("@name", "name");
                insertCommand.Parameters.AddWithValue("@value", Info.Name);
                insertCommand.ExecuteNonQuery();
                
                // description
                insertCommand.Parameters.Clear();
                insertCommand.Parameters.AddWithValue("@name", "description");
                insertCommand.Parameters.AddWithValue("@value", Info.Description);
                insertCommand.ExecuteNonQuery();
                
                // format
                insertCommand.Parameters.Clear();
                insertCommand.Parameters.AddWithValue("@name", "format");
                insertCommand.Parameters.AddWithValue("@value", Info.Format.ToString().ToLower());
                insertCommand.ExecuteNonQuery();
                
                // minzoom
                insertCommand.Parameters.Clear();
                insertCommand.Parameters.AddWithValue("@name", "minzoom");
                insertCommand.Parameters.AddWithValue("@value", Info.MinZoom.ToString());
                insertCommand.ExecuteNonQuery();
                
                // maxzoom
                insertCommand.Parameters.Clear();
                insertCommand.Parameters.AddWithValue("@name", "maxzoom");
                insertCommand.Parameters.AddWithValue("@value", Info.MaxZoom.ToString());
                insertCommand.ExecuteNonQuery();
                
                // bounds
                insertCommand.Parameters.Clear();
                insertCommand.Parameters.AddWithValue("@name", "bounds");
                insertCommand.Parameters.AddWithValue("@value", String.Join<double>(",", Info.Bounds));
                insertCommand.ExecuteNonQuery();
                
                // center
                if (Info.Center != null)
                {
                    insertCommand.Parameters.Clear();
                    insertCommand.Parameters.AddWithValue("@name", "center");
                    insertCommand.Parameters.AddWithValue("@value", String.Join<double>(",", Info.Center));
                    insertCommand.ExecuteNonQuery();
                }
                
                // json
                // serialize vector_layers

            }
        }
        
        public Tile GetTile(int z, int x, int y)
        {
            Tile tile = null;
            using (var connection = new SqliteConnection(ConnectionString))
            {
                connection.Open();
                var selectCommand = connection.CreateCommand();
                selectCommand.CommandText = "SELECT tile_data FROM tiles WHERE zoom_level = @z AND tile_column = @x AND tile_row = @y";
                selectCommand.Parameters.AddWithValue("@z", z);
                selectCommand.Parameters.AddWithValue("@x", x);
                selectCommand.Parameters.AddWithValue("@y", y);
                
                using (var reader = selectCommand.ExecuteReader())
                {
                    if (reader.Read() && !reader.IsDBNull(0))
                    {
                        byte[] buffer = reader.GetFieldValue<byte[]>(0);
                        tile = new Tile(buffer);
                    }
                }
            }
            return tile;
        }
        
        public void PutTile(int z, int x, int y, ITile tile)
        {
            // get hash
            string id;
            using (MD5 md5 = MD5.Create())
            {
                byte[] bytes = md5.ComputeHash(tile.Buffer);
                id = String.Join(String.Empty, bytes.Select(b => b.ToString("x2")));
            }
            
            // insert data
            using (var connection = new SqliteConnection(ConnectionString))
            {
                connection.Open();
                using (var transaction = connection.BeginTransaction())
                {
                    var imageCommand = connection.CreateCommand();
                    imageCommand.CommandText = "REPLACE INTO images (tile_data, tile_id) VALUES (@data, @id)";
                    imageCommand.Parameters.AddWithValue("@data", tile.Buffer);
                    imageCommand.Parameters.AddWithValue("@id", id);
                    imageCommand.ExecuteNonQuery();
                    
                    var mapCommand = connection.CreateCommand();
                    mapCommand.CommandText = "REPLACE INTO map (zoom_level, tile_column, tile_row, tile_id, grid_id)"
                        + "VALUES (@z,@x,@y,@id,(SELECT grid_id FROM map WHERE zoom_level = @gz AND tile_column = @gx AND tile_row = @gy))";
                    mapCommand.Parameters.AddWithValue("@z", z);
                    mapCommand.Parameters.AddWithValue("@x", x);
                    mapCommand.Parameters.AddWithValue("@y", y);
                    mapCommand.Parameters.AddWithValue("@id", id);
                    mapCommand.Parameters.AddWithValue("@gz", z);
                    mapCommand.Parameters.AddWithValue("@gx", x);
                    mapCommand.Parameters.AddWithValue("@gy", y);
                    mapCommand.ExecuteNonQuery(); 
                
                    transaction.Commit();
                }
            }  
        }
    }
}
