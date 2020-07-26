import org.apache.commons.io.IOUtils;
import java.io.*;
import java.sql.*;
import java.util.*;
import java.text.SimpleDateFormat;
import java.text.DateFormat;

public class ProjectStats extends Thread
{
	String prjGroup, project, repo, command, jdbcUrl;
	Connection conn;
	LinkedList<String> queue = new LinkedList<String>();

	public ProjectStats(String prjGroup, String project, String repo)
	{
		this.prjGroup = prjGroup;
		this.project  = project;
		this.repo     = repo;

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
		}catch (Exception e1)
		{
			System.out.println(e1.getMessage());
			e1.printStackTrace();
		}
	}
	
	public void run()
	{
		gitClone();
		extractLogs();
		importLogs();
		delClone();		
	}

	public void gitClone()
	{
		try
		{
			command = "git clone " + repo + " " + project;
			Process p = Runtime.getRuntime().exec(command);
			p.waitFor();
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	public void delClone()
	{
		try
		{
			command = "rm -Rf " + project;
			Process p = Runtime.getRuntime().exec(command);
			p.waitFor();
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	public void extractLogs()
	{
		try
		{
			command = "git --git-dir=" + project + "/.git log --stat";
			System.out.println(command);
			Process p = Runtime.getRuntime().exec(command);
			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line, log;
			line = reader.readLine();
			while(reader.ready()) 
			{
				log = line;
				line = reader.readLine();
				while ((line != null) && (line.indexOf("commit ") != 0))
				{
					log = log + "\n" + line;
					line = reader.readLine();
				}
				log = log.trim();
				queue.add(log);
//				System.out.println("--------------------");
//				System.out.println(log);
//				System.out.println("--------------------");
			}
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}


	public void importLogs()
	{
		String commit, author, email, user, domain, type;
		boolean merge;
		java.sql.Date date;
		String[] strArray, strArray2;
		int files, insertions, deletions, total;
		int pos1, pos2;
		SimpleDateFormat format = new SimpleDateFormat("EEE MMM d HH:mm:SS yyyy z");
		String pattern = "yyyy-MM-dd";
		DateFormat df = new SimpleDateFormat(pattern);
		String sql = "INSERT INTO commits (date, commit, author, email, user, domain, files, insertions, deletions, total, main_project, project, type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		
		try
		{
			conn = DriverManager.getConnection(jdbcUrl);
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

		// Handle each commit entry one by one
		for (String log : queue)
		{
			date   = new java.sql.Date(System.currentTimeMillis());
			commit = "N/A";
			author = "N/A";
			email  = "N/A";
			user   = "N/A";
			domain = "N/A";
			files      = 0;
			insertions = 0;
			deletions  = 0;
			total      = 0;
			merge = false;
			type  = "commit";

			try
			{
				String[] logs = log.split("\n");

				// commit id is in the first line;
				try
				{
					String commit_line = logs[0];
					strArray = commit_line.split(" "); 
					commit = strArray[1];
				} catch (Exception e) {}

				// second line might be merge or author
				// date line is after author line
				String author_line = logs[1];
				String date_line = logs[2];
				if (author_line.indexOf("Merge: ") == 0)
				{
					merge = true;
					type = "merge";
					author_line = logs[2];
					date_line = logs[3];
				}
				
				// parse author line
				try
				{
					author_line = author_line.substring(8);
					pos1 = author_line.indexOf(" <");
					author = author_line.substring(0, pos1);
					email = author_line.substring(pos1+2, author_line.length()-1);
					strArray = email.split("@"); 
					user = strArray[0];
					domain = strArray[1];
				} catch (Exception e) {}

				// parse date line
				try
				{
					pos1 = date_line.indexOf(":") + 1;
					date_line = date_line.substring(pos1).trim();
					java.util.Date test = format.parse(date_line);
					date = new java.sql.Date(test.getTime());
				} catch (Exception e) {}

				// if not merge, then summary is the last line
				if (!merge)
				{
					String update_line = logs[logs.length - 1];
					try
					{
						strArray = update_line.split(",");
						for (int i=0; i<strArray.length; i++)
						{
							strArray2 = strArray[i].trim().split(" ");
							if (strArray2[1].indexOf("file") == 0)
							{
								files = Integer.parseInt(strArray2[0]);
							}
							else if (strArray2[1].indexOf("insertion") == 0)
							{
								insertions = Integer.parseInt(strArray2[0]);
							}
							else if (strArray2[1].indexOf("deletion") == 0)
							{
								deletions = Integer.parseInt(strArray2[0]);
							}
						}
						total = insertions + deletions;
					} catch (Exception e) {}
				}

				// Write a record to database
//				System.out.println(date + "\t" + commit + "\t" + author + "\t" + email + "\t" + user + "\t" + domain + "\t" + files + "\t" + insertions + "\t" + deletions + "\t" + total);
				PreparedStatement preparedStatement = conn.prepareStatement(sql);
				preparedStatement.setDate(1, date);
				preparedStatement.setString(2, commit);
				preparedStatement.setString(3, author);
				preparedStatement.setString(4, email);
				preparedStatement.setString(5, user);
				preparedStatement.setString(6, domain);
				preparedStatement.setInt(7, files);
				preparedStatement.setInt(8, insertions);
				preparedStatement.setInt(9, deletions);
				preparedStatement.setInt(10, total);
				preparedStatement.setString(11, prjGroup);
				preparedStatement.setString(12, project);
				preparedStatement.setString(13, type);
				preparedStatement.executeUpdate();
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}		
		}
	}

	public static void main(String[] args)
	{
		ProjectStats prj = new ProjectStats(args[0], args[1], args[2]);
		prj.run();
	}
}
