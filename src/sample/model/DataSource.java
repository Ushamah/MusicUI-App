package sample.model;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataSource {
    public static final String DB_NAME = "music.db";
    public static final String CONNECTION_STRING = "jdbc:sqlite:C:\\javacourse\\src\\com\\ushwamala\\Masterclass\\Database\\Music\\" + DB_NAME;

    public static final String TABLE_ALBUMS = "albums";
    public static final String COLUMN_ALBUM_ID = "_id";
    public static final String COLUMN_ALBUM_NAME = "name";
    public static final String COLUMN_ALBUM_ARTIST = "artist";
    public static final int INDEX_ALBUM_ID = 1;
    public static final int INDEX_ALBUM_NAME = 2;
    public static final int INDEX_ALBUM_ARTIST = 3;

    public static final String TABLE_ARTISTS = "artists";
    public static final String COLUMN_ARTIST_ID = "_id";
    public static final String COLUMN_ARTIST_NAME = "name";
    public static final int INDEX_ARTIST_ID = 1;
    public static final int INDEX_ARTIST_NAME = 2;

    public static final String TABLE_SONGS = "songs";
    public static final String COLUMN_SONG_ID = "_id";
    public static final String COLUMN_SONG_TRACK = "track";
    public static final String COLUMN_SONG_TITLE = "title";
    public static final String COLUMN_SONG_ALBUM = "album";
    public static final int INDEX_SONG_ID = 1;
    public static final int INDEX_SONG_TRACK = 2;
    public static final int INDEX_SONG_TITLE = 3;
    public static final int INDEX_SONG_ALBUM = 4;

    public enum ORDER_BY {
        NONE,
        ASC,
        DESC
    }

    public static final String  UPDATE_ARTIST_NAME = "UPDATE " + TABLE_ARTISTS + " SET " +  COLUMN_ARTIST_NAME + " = ? WHERE " +
            COLUMN_ARTIST_ID + " = ?";

    public static final String QUERY_ARTISTS_START = "SELECT * FROM " + TABLE_ARTISTS;

    public static final String QUERY_ARTISTS_SORT = " ORDER BY " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + " COLLATE NOCASE ";

    public static final String QUERY_ALBUMS_BY_ARTIST_START =
            "SELECT " + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " FROM " + TABLE_ALBUMS +
                    " INNER JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST + " = " + TABLE_ARTISTS + "." + COLUMN_ARTIST_ID
                    + " WHERE " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + " = \"";

    public static final String QUERY_ALBUMS_BY_ARTIST_SORT = " ORDER BY " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + " COLLATE NOCASE ";

    /*SELECT artists.name, albums.name, songs.track FROM songs
    INNER JOIN albums ON songs.album = albums._id
    INNER JOIN artists ON albums.artist = artists._id
    WHERE songs.title = "Go Your Own Way"
    ORDER BY artists.name, albums.name COLLATE NOCASE ASC*/

    public static final String QUERY_ARTIST_FOR_SONG_START = "SELECT " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", " +
            TABLE_ALBUMS + "." + COLUMN_ARTIST_NAME + ", " + TABLE_SONGS + "." + COLUMN_SONG_TRACK + " FROM " +
            TABLE_SONGS + " INNER JOIN " + TABLE_ALBUMS + " ON " + TABLE_SONGS + "." + COLUMN_SONG_ALBUM +
            " = " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ID + " INNER JOIN " + TABLE_ARTISTS + " ON " +
            TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST + " = " + TABLE_ARTISTS + "." + COLUMN_ARTIST_ID
            + " WHERE " + TABLE_SONGS + "." + COLUMN_SONG_TITLE + " = \"";

    public static final String QUERY_ARTIST_FOR_SONG_SORT = " ORDER BY " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME
            + ", " + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " COLLATE NOCASE ";

    public static final String TABLE_ARTIST_SONG_VIEW = "artist_list";



   /* SELECT VIEW IF NOT EXISTS artist_list AS SELECT artists.name, albums.name AS album,
      songs.track, songs.title FROM songs INNER JOIN albums ON songs.album = albums._id
      INNER JOIN artists ON albums.artists = artists._id ORDER BY artists.name, albums.name, songs.track
   * */

    public static final String CREATE_ARTIST_FOR_SONG_VIEW = "CREATE VIEW IF NOT EXISTS " +
            TABLE_ARTIST_SONG_VIEW + " AS SELECT " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + "," +
            TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " AS " + COLUMN_SONG_ALBUM + ", " + TABLE_SONGS + "." + COLUMN_SONG_TRACK + "," +
            TABLE_SONGS + "." + COLUMN_SONG_TITLE + " FROM " + TABLE_SONGS + " INNER JOIN " + TABLE_ALBUMS + " ON " +
            TABLE_SONGS + "." + COLUMN_SONG_ALBUM + " = " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ID +
            " INNER JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST + " = " + TABLE_ARTISTS + "." + COLUMN_ARTIST_ID +
            " ORDER BY " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + "," + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + "," +
            TABLE_SONGS + "." + COLUMN_SONG_TRACK;

    //SELECT name, album, track from artist_list where title = "Go Your Own Way"
    public static final String QUERY_VIEW_SONG_INFO = "SELECT " + COLUMN_ARTIST_NAME + "," + COLUMN_SONG_ALBUM + "," + COLUMN_SONG_TRACK +
            " FROM " + TABLE_ARTIST_SONG_VIEW + " WHERE " + COLUMN_SONG_TITLE + " = \"";

    //Prepared statement
    //SELECT name, album, track from artist_list where title = ?
    public static final String QUERY_VIEW_SONG_INFO_PREP = "SELECT " + COLUMN_ARTIST_NAME + "," + COLUMN_SONG_ALBUM + "," + COLUMN_SONG_TRACK +
            " FROM " + TABLE_ARTIST_SONG_VIEW + " WHERE " + COLUMN_SONG_TITLE + " = ?";

    public static final String INSERT_ARTIST = "INSERT INTO " + TABLE_ARTISTS + "(" +
            COLUMN_ARTIST_NAME + ")VALUES (?)";

    public static final String INSERT_ALBUM = "INSERT INTO " + TABLE_ALBUMS + "(" +
            COLUMN_ALBUM_NAME + ", " + COLUMN_ALBUM_ARTIST + ") VALUES(?, ?)";

    public static final String INSERT_SONG = "INSERT INTO " + TABLE_SONGS + "(" +
            COLUMN_SONG_TRACK + ", " + COLUMN_SONG_TITLE + ", " + COLUMN_SONG_ALBUM + ") VALUES(?, ?, ?)";

    public static final String QUERY_ARTIST = "SELECT " + COLUMN_ARTIST_ID + " FROM " +
            TABLE_ARTISTS + " WHERE " + COLUMN_ARTIST_NAME + " = ?";

    public static final String QUERY_ALBUM = "SELECT " + COLUMN_ALBUM_ID + " FROM " +
            TABLE_ALBUMS + " WHERE " + COLUMN_ALBUM_NAME + " = ?";

    public static final String QUERY_ALBUMS_BY_ARTIST_ID = "SELECT * FROM " +
            TABLE_ALBUMS + " WHERE " + COLUMN_ALBUM_ARTIST + " = ? ORDER BY " + COLUMN_ALBUM_NAME + " COLLATE NOCASE";


    private Connection connection;

    //Step 1: Declare the prepared statement
    private PreparedStatement querySongInfoView_PrepStatement;

    private PreparedStatement insertIntoArtists_PreparedStatement;
    private PreparedStatement insertIntoAlbums_PreparedStatement;
    private PreparedStatement insertIntoSongs_PreparedStatement;

    private PreparedStatement queryArtist_PreparedStatement;
    private PreparedStatement queryAlbum_PreparedStatement;
    private PreparedStatement queryAlbumsByArtistId_PreparedStatement;
    private PreparedStatement updateArtistName_PreparedStatement;

    private static final DataSource instance = new DataSource();

    private DataSource() {
    }

    //Lazy instanciation - Read about it
    public static DataSource getInstance() {
        return instance;
    }


    public boolean open() {
        try {
            connection = DriverManager.getConnection(CONNECTION_STRING);
            // Step 2: Initialize the prepared statement in the open() method
            querySongInfoView_PrepStatement = connection.prepareStatement(QUERY_VIEW_SONG_INFO_PREP);

            //The second value in connection.preparedStatement() generates an id for us
            insertIntoArtists_PreparedStatement = connection.prepareStatement(INSERT_ARTIST, Statement.RETURN_GENERATED_KEYS);
            insertIntoAlbums_PreparedStatement = connection.prepareStatement(INSERT_ALBUM, Statement.RETURN_GENERATED_KEYS);
            insertIntoSongs_PreparedStatement = connection.prepareStatement(INSERT_SONG);

            queryArtist_PreparedStatement = connection.prepareStatement(QUERY_ARTIST);
            queryAlbum_PreparedStatement = connection.prepareStatement(QUERY_ALBUM);

            queryAlbumsByArtistId_PreparedStatement = connection.prepareStatement(QUERY_ALBUMS_BY_ARTIST_ID);

            updateArtistName_PreparedStatement = connection.prepareStatement(UPDATE_ARTIST_NAME);

            return true;
        } catch (SQLException e) {
            System.out.println("Couldn't connect to database: " + Arrays.toString(e.getStackTrace()));
            return false;
        }
    }

    public void close() {
        try {
            //Step 3: If the prepared statement is equal to null, close it before the connection in the close() method
            if (querySongInfoView_PrepStatement != null) {
                querySongInfoView_PrepStatement.close();
            }

            if (insertIntoArtists_PreparedStatement != null) {
                insertIntoArtists_PreparedStatement.close();
            }
            if (insertIntoAlbums_PreparedStatement != null) {
                insertIntoAlbums_PreparedStatement.close();
            }
            if (insertIntoSongs_PreparedStatement != null) {
                insertIntoSongs_PreparedStatement.close();
            }

            if (queryArtist_PreparedStatement != null) {
                queryArtist_PreparedStatement.close();
            }

            if (queryAlbum_PreparedStatement != null) {
                queryAlbum_PreparedStatement.close();
            }

            if (queryAlbumsByArtistId_PreparedStatement != null) {
                queryAlbumsByArtistId_PreparedStatement.close();
            }

            if (updateArtistName_PreparedStatement != null) {
                updateArtistName_PreparedStatement.close();
            }

            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            System.out.println("Couldn't close connection: " + Arrays.toString(e.getStackTrace()));
        }
    }



    public List<SongArtist> querySongInfoViewWithPrepStatement(String songTitle) {

        try {
            //Step 4: set the variables and their indexes using .setString(), .setInt(), etc.
            querySongInfoView_PrepStatement.setString(1, songTitle);

            //Step 5: Run the prepared statement using .execute() or .executeQuery() methods
            ResultSet resultSet = querySongInfoView_PrepStatement.executeQuery();

            List<SongArtist> songArtistList = createSongArtistList(resultSet);
            return songArtistList;

        } catch (SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
            return null;
        }
    }


    public boolean updateArtistName(int id, String newArtistName){
        try {
            updateArtistName_PreparedStatement.setString(1,newArtistName);
            updateArtistName_PreparedStatement.setInt(2,id);
            int affectedRecords = updateArtistName_PreparedStatement.executeUpdate();

            return affectedRecords == 1;

        } catch (SQLException e){
            System.out.println("Update failed: " + Arrays.toString(e.getStackTrace()));
            return false;
        }
    }

    public List<Album> queryAlbumsForArtistId(int id) {
        try {
            queryAlbumsByArtistId_PreparedStatement.setInt(1, id);
            ResultSet resultSet = queryAlbumsByArtistId_PreparedStatement.executeQuery();

            List<Album> albumsList = createAlbumsList(resultSet, id);
            return albumsList;
        } catch (SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
            return null;
        }

    }

    public List<Artist> queryArtists(ORDER_BY orderBy) {
        StringBuilder SQL_QUERY = new StringBuilder(QUERY_ARTISTS_START);
        sortBy(SQL_QUERY, orderBy, QUERY_ARTISTS_SORT);
        System.out.println(SQL_QUERY.toString());

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(SQL_QUERY.toString())) {


            List<Artist> artists = new ArrayList<>();
            while (resultSet.next()) {
                Artist artist = new Artist();

                //Using COLUMNS
                /*artist.setId(resultSet.getInt(COLUMN_ARTIST_ID));
                artist.setName(resultSet.getString(COLUMN_ARTIST_NAME));*/

                //Using INDEXES: This is more efficient while looping through big databases
                artist.setId(resultSet.getInt(INDEX_ARTIST_ID));
                artist.setName(resultSet.getString(INDEX_ARTIST_NAME));
                artists.add(artist);
            }
            return artists;
        } catch (SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
            return null;
        }
    }

    public List<String> queryAlbumsForArtist(String artistName, ORDER_BY orderBy) {
        StringBuilder SQL_QUERY = new StringBuilder(QUERY_ALBUMS_BY_ARTIST_START);
        SQL_QUERY.append(artistName).append("\"");
        sortBy(SQL_QUERY, orderBy, QUERY_ALBUMS_BY_ARTIST_SORT);

        System.out.println(SQL_QUERY.toString());

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(SQL_QUERY.toString())) {
            List<String> albums = new ArrayList<>();
            while (resultSet.next()) {
                albums.add(resultSet.getString(1));
            }
            return albums;

        } catch (SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
            return null;
        }
    }

    public List<SongArtist> queryArtistsForSong(String songName, ORDER_BY orderBy) {
        String SQL_QUERY = QUERY_ARTIST_FOR_SONG_START + songName + "\"";
        sortBy_String(SQL_QUERY, orderBy, QUERY_ARTIST_FOR_SONG_SORT);
        System.out.println();
        System.out.println(SQL_QUERY);

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(SQL_QUERY)) {

            List<SongArtist> songArtists = createSongArtistList(resultSet);
            return songArtists;

        } catch (SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
            return null;
        }
    }

    public void querySongsMetaDta() {
        String SQL_QUERY = "SELECT * FROM " + TABLE_SONGS;
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(SQL_QUERY)) {

            //TAKE A LOOK AT THE METADATA CLASS
            ResultSetMetaData metaData = resultSet.getMetaData();

            int numColumns = metaData.getColumnCount();
            for (int i = 1; i <= numColumns; i++) {
                System.out.format("Column %d in the songs table is names %s\n",
                        i, metaData.getColumnName(i));
            }
        } catch (SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
        }
    }

    public int getCount(String TABLE) {
        String SQL_QUERY = "SELECT COUNT(*) AS count FROM " + TABLE;
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(SQL_QUERY)) {

            //"count" comes from the SQL_QUERY
            return resultSet.getInt("count");

        } catch (SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
        }
        return -1;

        /*String SQL_QUERY = "SELECT COUNT(*) AS count, MIN(_id) AS min_id FROM " + TABLE;
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(SQL_QUERY)) {

            //count and min_id comes from the SQL_QUERY
            int count = resultSet.getInt("count");
            int min = resultSet.getInt("min_id");

            System.out.printf("Count = %d, Min = %d\n", count, min);
            return count;

        } catch (SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
        }
        return -1;*/
    }

    public boolean createViewForSongArtists() {
        try (Statement statement = connection.createStatement()) {
            statement.execute(CREATE_ARTIST_FOR_SONG_VIEW);
            return true;

        } catch (SQLException e) {
            System.out.println("Create View failed: " + e.getMessage());
            return false;
        }
    }


    //Creating a View
    public List<SongArtist> querySongInfoView(String songTitle) {

        StringBuilder SQL_QUERY = new StringBuilder(QUERY_VIEW_SONG_INFO);
        SQL_QUERY.append(songTitle).append("\"");

        System.out.println();
        System.out.println(SQL_QUERY.toString());

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(SQL_QUERY.toString())) {
            return createSongArtistList(resultSet);

        } catch (SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
            return null;
        }
    }


    private int insertArtist(String artistName) throws SQLException {
        queryArtist_PreparedStatement.setString(1, artistName);

        ResultSet resultSet = queryArtist_PreparedStatement.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt(1);
        } else {
            //Insert the artist
            insertIntoArtists_PreparedStatement.setString(1, artistName);

            int affectedRows = insertIntoArtists_PreparedStatement.executeUpdate();
            //The number of affectedRows has to be 1
            if (affectedRows != 1) {
                throw new SQLException("Couldn't insert artist");
            }

            ResultSet generatedKeys = insertIntoArtists_PreparedStatement.getGeneratedKeys();
            if (generatedKeys.next()) {
                return generatedKeys.getInt(1);
            } else {
                throw new SQLException("Couldn't get _id for artist");
            }
        }
    }

    private int insertAlbum(String albumName, int artistId) throws SQLException {
        queryAlbum_PreparedStatement.setString(1, albumName);

        ResultSet resultSet = queryAlbum_PreparedStatement.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt(1);
        } else {
            //Insert the album
            insertIntoAlbums_PreparedStatement.setString(1, albumName);
            insertIntoAlbums_PreparedStatement.setInt(2, artistId);

            int affectedRows = insertIntoAlbums_PreparedStatement.executeUpdate();
            if (affectedRows != 1) {
                throw new SQLException("Couldn't insert album");
            }

            ResultSet generatedKeys = insertIntoAlbums_PreparedStatement.getGeneratedKeys();
            if (generatedKeys.next()) {
                return generatedKeys.getInt(1);
            } else {
                throw new SQLException("Couldn't get _id for album");
            }
        }
    }

    public void insertSong(String songTitle, String artistName, String albumName, int track) {

        try {
            connection.setAutoCommit(false);

            int artistId = insertArtist(artistName);
            int albumId = insertAlbum(albumName, artistId);

            //Using an index which is greater than the number of rows will result into an IndexOutOfBoundException
            insertIntoSongs_PreparedStatement.setInt(1, track);
            insertIntoSongs_PreparedStatement.setString(2, songTitle);
            insertIntoSongs_PreparedStatement.setInt(3, albumId);

            int affectedRows = insertIntoSongs_PreparedStatement.executeUpdate();
            if (affectedRows == 1) {
                connection.commit();
            } else {
                throw new SQLException("The song insert failed");
            }
        } catch (Exception e) {
            System.out.println("Insert song exception: " + e.getMessage());
            try {
                System.out.println("Performing rollback...");
                connection.rollback();
            } catch (SQLException e2) {
                System.out.println("Oh Boy! Things are really bad! " + e2.getMessage());
            }
        } finally {
            try {
                System.out.println("Resetting default commit behaviour...");
                connection.setAutoCommit(true);
            } catch (SQLException e3) {
                System.out.println("Couldn't reset auto-commit! " + e3.getMessage());
            }
        }


    }


    private void sortBy(StringBuilder SQL_QUERY, ORDER_BY orderBy, String sortBy) {
        if (orderBy != ORDER_BY.NONE) {
            SQL_QUERY.append(sortBy);
            if (orderBy == ORDER_BY.DESC) {
                SQL_QUERY.append("DESC");
            } else {
                SQL_QUERY.append("ASC");
            }
        }
    }

    private void sortBy_String(String SQL_QUERY, ORDER_BY orderBy, String sortBy) {
        if (orderBy != ORDER_BY.NONE) {
            SQL_QUERY.concat(sortBy);
            if (orderBy == ORDER_BY.DESC) {
                SQL_QUERY.concat("DESC");
            } else {
                SQL_QUERY.concat("ASC");
            }
        }
    }

    private List<SongArtist> createSongArtistList(ResultSet resultSet) throws SQLException {
        List<SongArtist> songArtists = new ArrayList<>();
        while (resultSet.next()) {
            SongArtist songArtist = new SongArtist();
            songArtist.setArtistName(resultSet.getString(1));
            songArtist.setAlbumName(resultSet.getString(2));
            songArtist.setTrack(resultSet.getInt(3));
            songArtists.add(songArtist);
        }
        return songArtists;
    }


    private List<Album> createAlbumsList(ResultSet resultSet, int id) throws SQLException {
        List<Album> albums = new ArrayList<>();
        while (resultSet.next()) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                System.out.println("Interrupted: " + e.getMessage());
            }
            Album album = new Album();
            album.setId(resultSet.getInt(1));
            album.setName(resultSet.getString(2));
            album.setArtistID(id);
            albums.add(album);
        }
        return albums;
    }


}








