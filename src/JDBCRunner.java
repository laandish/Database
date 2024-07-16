import java.sql.*;

public class JDBCRunner {
    private final static String PROTOCOL = "jdbc:postgresql://";
    private final static String DRIVER = "org.postgresql.Driver";
    private final static String URL_LOCALE_NAME = "localhost/";

    private final static String DATABASE_NAME = "oceanariumDB";

    private final static String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
    private final static String USER_NAME = "postgres";
    private final static String DATABASE_PASSWORD = "postgres";

    public static void main(String[] args) {
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");
try (Connection connection = DriverManager.getConnection(DATABASE_URL,USER_NAME,DATABASE_PASSWORD)){
   getMammals(connection);System.out.println();
    getAquariums(connection);System.out.println();
    getOldestMammal(connection);System.out.println();
    getCertainVolumeAquariums(connection);System.out.println();
    getBiggestAquarium(connection);System.out.println();
    getAllTypesMammals(connection);System.out.println();

    getNutrition(connection);System.out.println();
    getMaxNutritionMammals(connection);System.out.println();

      addMammal(connection,"Лялиус","Ус",20);System.out.println();
    changeNicknameMammal(connection,"Лялиус","Ли");System.out.println();
    removeMammal(connection,"Лялиус");System.out.println();
}catch(SQLException e){
    if (e.getSQLState().startsWith("23")){
        System.out.println("Произошло дублирование данных");
    } else throw new RuntimeException(e);
}
    }
    public static void checkDriver(){
        try{
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e){
            System.out.println("Нет JDBS-драйвера!");
            throw new RuntimeException(e);
        }
    }
    public static void checkDB(){
        try{
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASSWORD);
        } catch (SQLException e){
            System.out.println("Нет базы данных или неправильные данные пароля и логина");
            throw new RuntimeException(e);
        }
    }

    private static void getAllTypesMammals(Connection connection) throws SQLException{
        String  columnName0 = "type";
        String param0 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT type FROM mammal;");
        while (rs.next()) {
            param0 = rs.getString(columnName0);
            System.out.println(param0);
        }
    }
    static void getMammals (Connection connection) throws SQLException {

        int param0 = -1, param3 = -1;
        String param1 = null,param2 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM mammal;");

        while (rs.next()) {
            param0 = rs.getInt(1);
            param1 = rs.getString(2);
            param2 = rs.getString(3);
            param3 = rs.getInt(4);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3);
        }
    }
    static void getAquariums (Connection connection) throws SQLException {

        int param0 = -1, param2 = -1;
        String param1 = null,param3 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM aquarium;");

        while (rs.next()) {
            param0 = rs.getInt(1);
            param1 = rs.getString(2);
            param2 = rs.getInt(3);
            param3 = rs.getString(4);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3);
        }
    }



private static void addMammal(Connection connection, String type, String nickname, int age)  throws SQLException {
    if (type == null || type.isBlank() || nickname == null || nickname.isBlank() || age < 0) return;

    PreparedStatement statement = connection.prepareStatement(
            "INSERT INTO mammal(type, nickname, age) VALUES (?, ?, ?) returning id;", Statement.RETURN_GENERATED_KEYS);
    statement.setString(1, type);
    statement.setString(2, nickname);
    statement.setInt(3, age);

    int count =
            statement.executeUpdate();

    ResultSet rs = statement.getGeneratedKeys();
    if (rs.next()) {
        System.out.println("Идентификатор млекопитающего " + rs.getInt(1));
    }

    System.out.println("Добавлено " + count + " млекопитающих");
  getMammals(connection);
}
    private static void changeNicknameMammal (Connection connection, String type, String nickname) throws SQLException {
        if (type == null || type.isBlank() || nickname == null || nickname.isBlank() ) return;

        PreparedStatement statement = connection.prepareStatement("UPDATE mammal SET nickname=? WHERE type=?;");
        statement.setString(1, nickname); //  что передаем
        statement.setString(2, type);   //  по чему ищем

        int count = statement.executeUpdate();

        System.out.println("Обновлено " + count + " млекопитающих");
        getMammals(connection);
    }
    private static void removeMammal(Connection connection, String type) throws SQLException {
        if (type == null || type.isBlank()) return;

        PreparedStatement statement = connection.prepareStatement("DELETE from mammal WHERE type=?;");
        statement.setString(1, type);

        int count = statement.executeUpdate();
        System.out.println("DELETEd " + count + " mammal");
       getMammals(connection);
    }


private static void getOldestMammal(Connection connection) throws SQLException{
    int param0 = -1, param3 = -1;
    String param1 = null,param2 = null;

    Statement statement = connection.createStatement();
    ResultSet rs = statement.executeQuery("SELECT * FROM mammal ORDER BY age DESC LIMIT 1;");

    while (rs.next()) {
        param0 = rs.getInt(1);
        param1 = rs.getString(2);
        param2 = rs.getString(3);
        param3 = rs.getInt(4);
        System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3);
    }
}
    static void getCertainVolumeAquariums (Connection connection) throws SQLException {

        int param0 = -1, param2 = -1;
        String param1 = null,param3 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM aquarium WHERE volume BETWEEN 15000 AND 50000;");

        while (rs.next()) {
            param0 = rs.getInt(1);
            param1 = rs.getString(2);
            param2 = rs.getInt(3);
            param3 = rs.getString(4);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3);
        }
    }
    private static void getBiggestAquarium(Connection connection) throws SQLException{
        int param0 = -1, param2 = -1;
        String param1 = null,param3 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM aquarium ORDER BY volume DESC LIMIT 1;");

        while (rs.next()) {
            param0 = rs.getInt(1);
            param1 = rs.getString(2);
            param2 = rs.getInt(3);
            param3 = rs.getString(4);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3);
        }
    }

private static void getNutrition(Connection connection) throws SQLException{
        String param0 = null,param1 = null, param2 = null, param3 = null;
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT mammal.type, mammal.nickname, aquarium.name,nutrition.feed_type FROM mammal JOIN nutrition ON mammal.id = nutrition.mammal_id JOIN aquarium ON aquarium.id = nutrition.aquarium_id ");

while(rs.next()){
    param0 = rs.getString(1);
    param1 = rs.getString(2);
    param2 = rs.getString(3);
    param3 = rs.getString(4);

    System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3);
}

    }

    private static void getMaxNutritionMammals(Connection connection) throws SQLException{
        int param0 = -1, param3 = -1;
        String param1 = null,param2 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT mammal.id, mammal.type, mammal.nickname,mammal.age,nutrition.quantity FROM mammal JOIN nutrition ON mammal.id = nutrition.mammal_id ORDER BY nutrition.quantity DESC LIMIT 1 ");

        while (rs.next()) {
            param0 = rs.getInt(1);
            param1 = rs.getString(2);
            param2 = rs.getString(3);
            param3 = rs.getInt(4);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3);
        }
    }

}
