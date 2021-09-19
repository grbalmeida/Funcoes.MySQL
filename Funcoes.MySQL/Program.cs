using MySql.Data.MySqlClient;
using System;
using System.Configuration;
using System.Text;

namespace Funcoes.MySQL
{
    class Program
    {
        private static readonly string _connectionString = ConfigurationManager.ConnectionStrings["DefaultConnection"].ToString();

        static void Main()
        {
            // Funções Numéricas
            Round();
            Truncate();

            // Funções String
            Concat();
            Concat_WS();
            Lower();
            Upper();
            Substring();
            Left();
            Replace();

            // Funções Data
            Month();
            MonthName();
            DayName();
            DayOfMonth();
            DayOfWeek();
            DayOfYear();
            DateDiff();
            SubDate();

            // Funções de Agregação
            Avg();
            Count();
            Sum();
            Max();
            Min();

            GroupByAvgSum();
            GroupByCountMax();

            HavingAvgSum();
            HavingCountMax();

            Console.ReadLine();
        }

        static void HavingCountMax()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            MySqlCommand comando;

            CreateTableFuncionarios(connection);

            var cmdText = "SELECT DepartamentoId, COUNT(*), MAX(Salario) FROM Funcionarios WHERE DepartamentoId > 1 GROUP BY(DepartamentoId) HAVING MAX(Salario) > 5000";

            comando = new MySqlCommand(cmdText, connection);

            var reader = comando.ExecuteReader();

            while (reader.Read())
            {
                var departamentoId = reader.GetInt32(0);
                var count = reader.GetDecimal(1);
                var max = reader.GetDecimal(2);

                Console.WriteLine($"DepartamentoId: {departamentoId}");
                Console.WriteLine($"Count: {count}");
                Console.WriteLine($"Max: {max}");
                Console.WriteLine();
            }

            // DepartamentoId: 4
            // Count: 2
            // Max: 5558.10

            reader.Dispose();

            DropTableFuncionarios(connection);

            Console.WriteLine(string.Join('-', new string[30]));

            connection.Close();
        }

        static void HavingAvgSum()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            MySqlCommand comando;

            CreateTableFuncionarios(connection);

            var cmdText = "SELECT DepartamentoId, AVG(Salario), SUM(Salario) FROM Funcionarios GROUP BY(DepartamentoId) HAVING SUM(Salario) < 5000";

            comando = new MySqlCommand(cmdText, connection);

            var reader = comando.ExecuteReader();

            while (reader.Read())
            {
                var departamentoId = reader.GetInt32(0);
                var avg = reader.GetDecimal(1);
                var sum = reader.GetDecimal(2);

                Console.WriteLine($"DepartamentoId: {departamentoId}");
                Console.WriteLine($"Avg: {avg}");
                Console.WriteLine($"Sum: {sum}");
                Console.WriteLine();
            }

            // DepartamentoId: 3
            // Avg: 1987.610000
            // Sum: 3975.22

            reader.Dispose();

            DropTableFuncionarios(connection);

            Console.WriteLine(string.Join('-', new string[30]));

            connection.Close();
        }

        static void GroupByCountMax()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            MySqlCommand comando;

            CreateTableFuncionarios(connection);

            var cmdText = "SELECT DepartamentoId, COUNT(*), MAX(Salario) FROM Funcionarios WHERE DepartamentoId > 1 GROUP BY DepartamentoId";

            comando = new MySqlCommand(cmdText, connection);

            var reader = comando.ExecuteReader();

            while (reader.Read())
            {
                var departamentoId = reader.GetInt32(0);
                var count = reader.GetDecimal(1);
                var max = reader.GetDecimal(2);

                Console.WriteLine($"DepartamentoId: {departamentoId}");
                Console.WriteLine($"Count: {count}");
                Console.WriteLine($"Max: {max}");
                Console.WriteLine();
            }

            // DepartamentoId: 4
            // Count: 2
            // Max: 5558.10

            // DepartamentoId: 5
            // Count: 2
            // Max: 3562.34

            // DepartamentoId: 3
            // Count: 2
            // Max: 2000.87

            reader.Dispose();

            DropTableFuncionarios(connection);

            Console.WriteLine(string.Join('-', new string[30]));

            connection.Close();
        }

        static void GroupByAvgSum()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            MySqlCommand comando;

            CreateTableFuncionarios(connection);

            var cmdText = "SELECT DepartamentoId, AVG(Salario), SUM(Salario) FROM Funcionarios GROUP BY DepartamentoId";

            comando = new MySqlCommand(cmdText, connection);

            var reader = comando.ExecuteReader();

            while (reader.Read())
            {
                var departamentoId = reader.GetInt32(0);
                var media = reader.GetDecimal(1);
                var soma = reader.GetDecimal(2);

                Console.WriteLine($"DepartamentoId: {departamentoId}");
                Console.WriteLine($"Média: {media}");
                Console.WriteLine($"Soma: {soma}");
                Console.WriteLine();
            }

            // DepartamentoId: 4
            // Média: 4108.645000
            // Soma: 8217.29

            // DepartamentoId: 5
            // Média: 3173.750000
            // Soma: 6347.50

            // DepartamentoId: 3
            // Média: 1987.610000
            // Soma: 3975.22

            reader.Dispose();

            DropTableFuncionarios(connection);

            Console.WriteLine(string.Join('-', new string[30]));

            //comando.Dispose();
            connection.Close();
        }

        private static void DropTableFuncionarios(MySqlConnection connection)
        {
            var cmdText = "DROP TABLE Funcionarios";

            var comando = new MySqlCommand(cmdText, connection);
            comando.ExecuteNonQuery();
            comando.Dispose();
        }

        private static void CreateTableFuncionarios(MySqlConnection connection)
        {
            var sql = new StringBuilder();

            sql.AppendLine("CREATE TEMPORARY TABLE Funcionarios (");
            sql.AppendLine("  Id INT NOT NULL,");
            sql.AppendLine("  Nome VARCHAR(100) NOT NULL,");
            sql.AppendLine("  Sobrenome VARCHAR(100) NOT NULL,");
            sql.AppendLine("  Endereco VARCHAR(200),");
            sql.AppendLine("  DataNascimento DATETIME NOT NULL,");
            sql.AppendLine("  Salario DECIMAL(10, 2) NOT NULL,");
            sql.AppendLine("  Sexo CHAR NOT NULL,");
            sql.AppendLine("  GerenteId INT,");
            sql.AppendLine("  DepartamentoId INT,");
            sql.AppendLine("  PRIMARY KEY(Id));");

            var comando = new MySqlCommand(sql.ToString(), connection);
            comando.ExecuteNonQuery();

            sql.Clear();

            sql.AppendLine("INSERT INTO Funcionarios");
            sql.AppendLine("(Id, Nome, Sobrenome, Endereco, DataNascimento, Salario, Sexo, GerenteId, DepartamentoId)");
            sql.AppendLine("VALUES");
            sql.AppendLine("(1163, 'Claudia', 'Morais', 'Rua A', '1974-08-12', 5558.10, 'F', NULL, 4),");
            sql.AppendLine("(1164, 'Jorge', 'Vila Verde', 'Rua B', '1985-01-25', 3562.34, 'M', 1163, 5),");
            sql.AppendLine("(1165, 'Marina', 'Lobo', 'Rua C', '1996-09-13', 2785.16, 'F', 1164, 5),");
            sql.AppendLine("(1166, 'Moacir', 'Junqueira', 'Rua D', '1991-12-30', 2659.19, 'M', 1165, 4),");
            sql.AppendLine("(1167, 'Joana', 'Valério', 'Rua E', '1970-05-24', 2000.87, 'F', NULL, 3),");
            sql.AppendLine("(1168, 'Bete', 'Rodrigues', 'Rua F', '1979-11-15', 1974.35, 'F', NULL, 3)");

            comando = new MySqlCommand(sql.ToString(), connection);
            comando.ExecuteNonQuery();

            comando.Dispose();
        }

        static void Min()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            MySqlCommand comando;

            CreateTableProdutos(connection);

            comando = new MySqlCommand("SELECT MIN(Preco) FROM Produtos", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 1.45

            comando = new MySqlCommand("SELECT MIN(Preco) FROM Produtos WHERE Preco > 10", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 10.34

            DropTableProdutos(connection);

            Console.WriteLine(string.Join('-', new string[30]));

            comando.Dispose();
            connection.Close();
        }

        static void Max()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            MySqlCommand comando;

            CreateTableProdutos(connection);

            comando = new MySqlCommand("SELECT MAX(Preco) FROM Produtos", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 35.39

            comando = new MySqlCommand("SELECT MAX(Preco) FROM Produtos WHERE Preco < 30", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 11.46

            DropTableProdutos(connection);

            Console.WriteLine(string.Join('-', new string[30]));

            comando.Dispose();
            connection.Close();
        }

        static void Sum()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            MySqlCommand comando;

            CreateTableProdutos(connection);

            comando = new MySqlCommand("SELECT SUM(Preco) FROM Produtos", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 58.64

            comando = new MySqlCommand("SELECT SUM(Preco) FROM Produtos WHERE Preco < 30", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 23.25

            DropTableProdutos(connection);

            Console.WriteLine(string.Join('-', new string[30]));

            comando.Dispose();
            connection.Close();
        }

        static void Count()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            MySqlCommand comando;

            CreateTableProdutos(connection);

            comando = new MySqlCommand("SELECT COUNT(Preco) FROM Produtos", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 4

            comando = new MySqlCommand("SELECT COUNT(Preco) FROM Produtos WHERE Preco < 30", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 3

            DropTableProdutos(connection);

            Console.WriteLine(string.Join('-', new string[30]));

            comando.Dispose();
            connection.Close();
        }

        static void Avg()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            MySqlCommand comando;

            CreateTableProdutos(connection);

            comando = new MySqlCommand("SELECT AVG(Preco) FROM Produtos", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 14.66

            comando = new MySqlCommand("SELECT AVG(Preco) FROM Produtos WHERE Preco < 30", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 7.75

            DropTableProdutos(connection);

            Console.WriteLine(string.Join('-', new string[30]));

            comando.Dispose();
            connection.Close();
        }

        private static void CreateTableProdutos(MySqlConnection connection)
        {
            var createTableScript = new StringBuilder();
            createTableScript.AppendLine("CREATE TEMPORARY TABLE Produtos (");
            createTableScript.AppendLine("  Preco DECIMAL (10, 2) NOT NULL");
            createTableScript.AppendLine(");");

            var comando = new MySqlCommand(createTableScript.ToString(), connection);
            comando.ExecuteNonQuery();

            comando = new MySqlCommand("INSERT INTO Produtos VALUES(10.34), (11.46), (35.39), (1.45)", connection);
            comando.ExecuteNonQuery();
            comando.Dispose();
        }

        private static void DropTableProdutos(MySqlConnection connection)
        {
            var comando = new MySqlCommand("DROP TABLE Produtos", connection);
            comando.ExecuteNonQuery();
            comando.Dispose();
        }

        static void SubDate()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            MySqlCommand comando;

            comando = new MySqlCommand("SELECT SUBDATE('2021-10-30', INTERVAL 20 DAY)", connection);
            Console.WriteLine(comando.ExecuteScalar()); // '2021-10-10'

            comando = new MySqlCommand("SELECT SUBDATE('2021-10-30', INTERVAL 1 WEEK)", connection);
            Console.WriteLine(comando.ExecuteScalar()); // '2021-10-23'

            comando = new MySqlCommand("SELECT SUBDATE('2021-10-30', INTERVAL 1 MONTH)", connection);
            Console.WriteLine(comando.ExecuteScalar()); // '2021-09-30'

            comando = new MySqlCommand("SELECT SUBDATE('2021-10-30', INTERVAL 60 * 60 * 24 * 3 SECOND)", connection);
            Console.WriteLine(comando.ExecuteScalar()); // '2021-10-27 00:00:00'

            comando = new MySqlCommand("SELECT SUBDATE('2021-10-30', INTERVAL 60 * 24 * 5 MINUTE)", connection);
            Console.WriteLine(comando.ExecuteScalar()); // '2021-10-25 00:00:00'

            Console.WriteLine(string.Join('-', new string[30]));

            comando.Dispose();
            connection.Close();
        }

        static void DateDiff()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            var comando = new MySqlCommand("SELECT DATEDIFF('2021-01-08', '2021-01-03')", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 5

            Console.WriteLine(string.Join('-', new string[30]));

            comando.Dispose();
            connection.Close();
        }

        static void DayOfYear()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            var comando = new MySqlCommand("SELECT DAYOFYEAR('2021-01-08')", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 8

            Console.WriteLine(string.Join('-', new string[30]));

            comando.Dispose();
            connection.Close();
        }

        static void DayOfWeek()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            var comando = new MySqlCommand("SELECT DAYOFWEEK('2021-09-18')", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 7 == Saturday

            Console.WriteLine(string.Join('-', new string[30]));

            comando.Dispose();
            connection.Close();
        }

        static void DayOfMonth()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            var comando = new MySqlCommand("SELECT DAYOFMONTH('2021-09-15')", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 15

            Console.WriteLine(string.Join('-', new string[30]));

            comando.Dispose();
            connection.Close();
        }

        static void DayName()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            MySqlCommand comando;

            comando = new MySqlCommand("SELECT DAYNAME('2021-09-18')", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 'Saturday'

            comando = new MySqlCommand("SELECT DAYNAME('2021-09-19')", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 'Sunday'

            comando = new MySqlCommand("SELECT DAYNAME('2021-09-20')", connection);
            Console.WriteLine(comando.ExecuteScalar()); // Monday

            comando = new MySqlCommand("SELECT DAYNAME('2021-09-21')", connection);
            Console.WriteLine(comando.ExecuteScalar()); // Tuesday

            Console.WriteLine(string.Join('-', new string[30]));

            comando.Dispose();
            connection.Close();
        }

        static void MonthName()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            MySqlCommand comando;

            comando = new MySqlCommand("SELECT MONTHNAME('2008-01-10')", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 'January'

            comando = new MySqlCommand("SELECT MONTHNAME('2008-02-10')", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 'February'

            comando = new MySqlCommand("SELECT MONTHNAME('2008-03-10')", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 'March'

            comando = new MySqlCommand("SELECT MONTHNAME('2008-04-10')", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 'April'

            Console.WriteLine(string.Join('-', new string[30]));

            comando.Dispose();
            connection.Close();
        }

        static void Month()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            MySqlCommand comando;

            comando = new MySqlCommand("SELECT MONTH('2008-01-10')", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 1

            comando = new MySqlCommand("SELECT MONTH('2008-02-10')", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 2

            comando = new MySqlCommand("SELECT MONTH('2008-03-10')", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 3

            comando = new MySqlCommand("SELECT MONTH('2008-04-10')", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 4

            Console.WriteLine(string.Join('-', new string[30]));

            comando.Dispose();
            connection.Close();
        }

        static void Replace()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            var comando = new MySqlCommand("SELECT REPLACE('www.mysql.com', 'w', 'WW')", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 'WWWWWW.mysql.com'

            Console.WriteLine(string.Join('-', new string[30]));

            comando.Dispose();
            connection.Close();
        }

        static void Left()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            var comando = new MySqlCommand("SELECT LEFT('foobarbar', 5)", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 'fooba'

            Console.WriteLine(string.Join('-', new string[30]));

            comando.Dispose();
            connection.Close();
        }

        static void Substring()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            MySqlCommand comando;

            comando = new MySqlCommand("SELECT SUBSTRING('Quadratically', 5)", connection);
            Console.WriteLine(comando.ExecuteScalar()); // ratically

            comando = new MySqlCommand("SELECT SUBSTRING('foobarbar' FROM 4)", connection);
            Console.WriteLine(comando.ExecuteScalar()); // barbar

            comando = new MySqlCommand("SELECT SUBSTRING('Quadratically', 5, 6)", connection);
            Console.WriteLine(comando.ExecuteScalar()); // ratica

            comando = new MySqlCommand("SELECT SUBSTRING('Sakila', -3)", connection);
            Console.WriteLine(comando.ExecuteScalar()); // ila

            comando = new MySqlCommand("SELECT SUBSTRING('Sakila', -5, 3)", connection);
            Console.WriteLine(comando.ExecuteScalar()); // aki

            comando = new MySqlCommand("SELECT SUBSTRING('Sakila' FROM -4 FOR 2)", connection);
            Console.WriteLine(comando.ExecuteScalar()); // ki

            Console.WriteLine(string.Join('-', new string[30]));

            comando.Dispose();
            connection.Close();
        }

        static void Upper()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            var comando = new MySqlCommand("SELECT UPPER ('Hej')", connection);

            Console.WriteLine(comando.ExecuteScalar()); // 'HEJ'
            Console.WriteLine(string.Join('-', new string[30]));

            comando.Dispose();
            connection.Close();
        }

        static void Lower()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            var comando = new MySqlCommand("SELECT LOWER ('QUADRATICALLY')", connection);

            Console.WriteLine(comando.ExecuteScalar()); // 'quadratically'
            Console.WriteLine(string.Join('-', new string[30]));

            comando.Dispose();
            connection.Close();
        }

        static void Concat_WS()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            MySqlCommand comando;

            comando = new MySqlCommand("SELECT CONCAT_WS (',', 'First Name', 'Second Name', 'Last Name')", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 'First Name,Second Name,Last Name'

            comando = new MySqlCommand("SELECT CONCAT_WS (',', 'First Name', NULL, 'Last Name')", connection);
            Console.WriteLine(comando.ExecuteScalar());

            Console.WriteLine(string.Join('-', new string[30]));

            comando.Dispose();
            connection.Close();
        }

        static void Concat()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            MySqlCommand comando;

            comando = new MySqlCommand("SELECT CONCAT ('My', 'S', 'QL')", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 'MySQL'

            comando = new MySqlCommand("SELECT CONCAT ('My', NULL, 'QL')", connection);
            Console.WriteLine(comando.ExecuteScalar()); // NULL

            comando = new MySqlCommand("SELECT CONCAT (14.3)", connection);
            Console.WriteLine(comando.ExecuteScalar()); // '14.3'

            Console.WriteLine(string.Join('-', new string[30]));

            comando.Dispose();
            connection.Close();
        }

        static void Round()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            MySqlCommand comando;

            comando = new MySqlCommand("SELECT ROUND (-1.23)", connection);
            Console.WriteLine(comando.ExecuteScalar()); // -1

            comando = new MySqlCommand("SELECT ROUND (-1.58)", connection);
            Console.WriteLine(comando.ExecuteScalar()); // -2

            comando = new MySqlCommand("SELECT ROUND (1.58)", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 2

            comando = new MySqlCommand("SELECT ROUND (1.298, 1)", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 1.3

            comando = new MySqlCommand("SELECT ROUND (1.298, 0)", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 1

            comando = new MySqlCommand("SELECT ROUND (23.298, -1)", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 20

            Console.WriteLine(string.Join('-', new string[30]));

            comando.Dispose();
            connection.Close();
        }

        static void Truncate()
        {
            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            MySqlCommand comando;

            comando = new MySqlCommand("SELECT TRUNCATE (1.223, 1)", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 1.2

            comando = new MySqlCommand("SELECT TRUNCATE (1.999, 1)", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 1.9

            comando = new MySqlCommand("SELECT TRUNCATE (1.999, 0)", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 1

            comando = new MySqlCommand("SELECT TRUNCATE (-1.999, 1)", connection);
            Console.WriteLine(comando.ExecuteScalar()); // -1.9

            comando = new MySqlCommand("SELECT TRUNCATE (122, -2)", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 100

            comando = new MySqlCommand("SELECT TRUNCATE (10.28 * 100, 0)", connection);
            Console.WriteLine(comando.ExecuteScalar()); // 1028

            Console.WriteLine(string.Join('-', new string[30]));

            comando.Dispose();
            connection.Close();
        }
    }
}
