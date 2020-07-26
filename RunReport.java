import org.apache.commons.io.IOUtils;
import java.io.*;
import java.sql.*;
import java.util.*;
import java.text.SimpleDateFormat;
import java.text.DateFormat;

public class RunReport
{
	String project, jdbcUrl;
	Connection conn;
	Statement stmt;
	ResultSet rs;	
	String sql = "SELECT * FROM projects LIMIT 1";
	
	public RunReport(String project)
	{
		this.project = project;
		try 
		{
			// Getting database properties from db.properties
			Properties prop = new Properties();
			InputStream input = new FileInputStream("config.properties");
			prop.load(input);
			String db_hostname = prop.getProperty("db_hostname");
			String db_username = prop.getProperty("db_username");
			String db_password = prop.getProperty("db_password");
			String db_database = prop.getProperty("db_database");
			// Load the MySQL JDBC driver
			Class.forName("com.mysql.jdbc.Driver");
			jdbcUrl = "jdbc:mysql://" + db_hostname + "/" + db_database + "?user=" + db_username + "&password=" + db_password;
			conn = DriverManager.getConnection(jdbcUrl);
			stmt = conn.createStatement();
		}catch (Exception e1)
		{
			System.out.println(e1.getMessage());
			e1.printStackTrace();
		}
	}
	
	public void run()
	{
		try
		{
			int i = 0, total = 0, max=100;
			int[] years = new int[max];
			int[] commits = new int[max];
			int[] authors = new int[max];
			int[] orgs = new int[max];
			int[] productivity = new int[max];
			double[] simpson = new double[max];
			double[] diversity = new double[max];
			double[] strength = new double[max];
			double[] prod_ave = new double[max];
						
			sql = getSql("commits");
			rs = stmt.executeQuery(sql);
			i = 0;
			while (rs.next()) 
			{
				years[i] = rs.getInt("year");
				commits[i] = rs.getInt("count");
				i++;
			}	
			total = i;		

			sql = getSql("authors");
			rs = stmt.executeQuery(sql);
			i = 0;
			while (rs.next()) 
			{
				authors[i] = rs.getInt("count");
				i++;
			}			

			sql = getSql("orgs");
			rs = stmt.executeQuery(sql);
			i = 0;
			while (rs.next()) 
			{
				orgs[i] = rs.getInt("count");
				i++;
			}			

			sql = getSql("strength");
			rs = stmt.executeQuery(sql);
			i = 0;
			while (rs.next()) 
			{
				strength[i] = 6.25 * (double) rs.getInt("total") / (double) rs.getInt("count");
				productivity[i] = rs.getInt("total");
				prod_ave[i] = 6.25 * (double) rs.getInt("total") / (double) authors[i];
				i++;
			}			
			
			for (i=0; i<total; i++)
			{
				simpson[i] = getSimpson(years[i]);
				diversity[i] = 1 / simpson[i];
			}

			
			System.out.println("Year\tCommit\tAuthor\tOrgs\tSimpson\tDiv.\tStreng\tProd\tAveProd");
			for (i=0; i<total; i++)
			{
				System.out.printf("%d\t%d\t%d\t%d\t%.2f\t%.2f\t%.2f\t%d\t%.2f\n", years[i], commits[i], authors[i], orgs[i], simpson[i], diversity[i], strength[i], productivity[i], prod_ave[i]);
			}

		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
	
	public String getSql(String type)
	{
		if (type.equals("commits"))
		{
			sql = "SELECT COUNT(*) AS count, YEAR(date) AS year FROM commits WHERE project='" + project + "' GROUP BY year ORDER BY year";
		}
		else if (type.equals("authors"))
		{
			sql = "SELECT COUNT(DISTINCT(email)) AS count, YEAR(date) AS year FROM commits WHERE project='" + project + "' GROUP BY year ORDER BY year";
		}
		else if (type.equals("orgs"))
		{
			sql = "SELECT COUNT(DISTINCT(domain)) AS count, YEAR(date) AS year FROM commits WHERE project='" + project + "' GROUP BY year ORDER BY year";
		}
		else if (type.equals("strength"))
		{
			sql = "SELECT SUM(strength) AS total, COUNT(*) AS count, YEAR(date) AS year FROM commits WHERE project='" + project + "' GROUP BY year ORDER BY year";
		}
		
		
		return sql;
	}
	
	public double getSimpson(int year)
	{
		int total = 0, count = 0, i;
		double diversity = 0.0;
		try
		{
			sql = "SELECT COUNT(*) AS count FROM commits WHERE project='" + project +"' AND YEAR(date) = " + year;
			rs = stmt.executeQuery(sql);
			while (rs.next()) 
			{
					total = rs.getInt("count");
//					System.out.println(year + "\t" + total);
			}	
			
			if (total != 0)
			{
				sql = "SELECT COUNT(*) AS count, domain AS org FROM commits WHERE project='" + project +"' AND YEAR(date) = " + year + " GROUP BY org ORDER BY count DESC";
				rs = stmt.executeQuery(sql);
				i = 0;
				while (rs.next() && (i<10)) 
				{
						count = rs.getInt("count");
						double percent = (double) count / (double) total;
						diversity = diversity + percent * percent;
//						System.out.println(count + "\t" + diversity);
						i++;
				}	
			}
		}catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

		return diversity;
	}	
	
	public void runCumu()
	{
		try
		{
			int total = 1;
			sql = "SELECT COUNT(*) AS count FROM commits";
			rs = stmt.executeQuery(sql);
			while (rs.next()) 
			{
				total = rs.getInt("count");
			}

			int count = 0;
			int lines = 0;
			double cumu = 0.0;
			sql = "SELECT COUNT(*) AS count, total FROM commits GROUP BY total ORDER BY total ASC";
			rs = stmt.executeQuery(sql);
			while (rs.next()) 
			{
				count = rs.getInt("count");
				lines = rs.getInt("total");
				cumu = cumu + ((double) count / (double) total);
				System.out.println(lines + "\t" + count + "\t" + cumu);
			}
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}		
	}
	public static void main(String[] args)
	{
		RunReport report = new RunReport(args[0]);
		 report.run();
		//report.runCumu();
	}
}
