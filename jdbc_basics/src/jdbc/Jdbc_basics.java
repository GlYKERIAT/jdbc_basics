package jdbc;
import java.sql.*;


public class Jdbc_basics {
	
	private static final String dbUrl = "jdbc:mysql://localhost:3306/demo";
	private static final String user = "user01";		
	private static final String pass = "user01";
	
	public static void main(String[] args) throws SQLException {
		//query in base
		String myQuery = "select * from employees ";
		mySqlQuery(dbUrl, user, pass, myQuery);
		
		//insert rows in the table
		System.out.println("Insert rows in the table");
    	String myQuery1 ="INSERT INTO employees (id,last_name,first_name,email, department, salary) VALUES (17,'Glyk','Eria','john.doe@foo.com', 'Developer', 70000.00)";
    	myUpdate(dbUrl, user, pass,myQuery1);
    	
    	//update rows in the table change the email
    	System.out.println("Udate email for Glyk Eria person");
		String myQuery11 ="update employees " +
				"set email='glyk@email.com' " + 
				"where last_name='Glyk' and first_name='Eria'";
		myUpdate(dbUrl, user, pass,myQuery11);
		
		//delete rows in the table
		System.out.println("Delete Joe Davis");
		String myQuery12 = "delete from employees " +
			"where last_name='Davis' and first_name='John'";
		myUpdate(dbUrl, user, pass,myQuery12);
		
		//use Prepared method
		Double minSalary= 45000.0;
		String department = "HR";
		myPrepared(dbUrl, user, pass, minSalary, department);
		
		//use Prepared method
		Double minSalary1= 45000.0;
		String department1 = "Engineering";
		myPrepared(dbUrl, user, pass, minSalary1, department1);

    	//stored procedures
    	System.out.println("Stored procedures");
    	String departement = "Legal";
    	Double increaseAmount = 100.0;
    	mystoredProcedure(dbUrl, user, pass, departement, increaseAmount);
    	
		//show the different 
		System.out.println("Display the new table");
    	mySqlQuery(dbUrl, user, pass, myQuery);
    	
	}
	
//  display simple sql queries	
	private static void mySqlQuery(String dbUrl, String user, String pass, String myQuery ) throws SQLException {
		try (Connection myConn = DriverManager.getConnection(dbUrl, user, pass);
			     Statement myStmt = myConn.createStatement();
			     ResultSet myRs = myStmt.executeQuery(myQuery)) {

			    while (myRs.next()) {
			    	String theLastName = myRs.getString("last_name");
					String theFirstName = myRs.getString("first_name");
					String email = myRs.getString("email");
					String salary = myRs.getString("salary");
					String dep = myRs.getString("department");
				
					System.out.printf(" %s %s, %s, %s\n", theFirstName, theLastName, email, salary, dep);
			    }

			} catch (Exception exc) {
			    exc.printStackTrace();
			}
	}
	
//	insert/update/delete rows in the table	
	private static void myUpdate(String dbUrl, String user, String pass, String myQuery ) throws SQLException {
		try (Connection myConn = DriverManager.getConnection(dbUrl, user, pass);
			     Statement myStmt = myConn.createStatement();) {
			
			int rowsEffected = myStmt.executeUpdate(myQuery);
			System.out.println("The rows which effected is, " + rowsEffected);	

			} catch (SQLIntegrityConstraintViolationException e) {
			    System.out.println("The person is already exist with this id");
			}catch (Exception exc) {
			    exc.printStackTrace();
			}	
	}
	
	private static void myPrepared(String dbUrl, String user, String pass, Double minSalary, String department ) throws SQLException { 		
		try (Connection myConn = DriverManager.getConnection(dbUrl, user, pass);
			     PreparedStatement myStmt = myConn.prepareStatement("select * from employees where salary > ? and department=?")) {
	
			    // Set the parameters
			    myStmt.setDouble(1, minSalary);
			    myStmt.setString(2, department);
			    ResultSet myRs =myStmt.executeQuery();

			    System.out.printf("Prepared method show employees from departement %s, with salary > %s\n", department, minSalary);
			       
			    while (myRs.next()) {
			    	String theLastName = myRs.getString("last_name");
					String theFirstName = myRs.getString("first_name");
					String email = myRs.getString("email");
					String depart = myRs.getString("department");
				
					System.out.printf(" %s %s, %s, %s\n", theFirstName, theLastName, email, depart);
			
			    }
				if (myRs != null) {
					myRs.close();
				}

			} catch (Exception exc) {
			    exc.printStackTrace();
		
			}			
	}
	
//	stored procedure	
	private static void mystoredProcedure(String dbUrl, String user,String pass, String departement, Double increaseAmount) throws SQLException {
		try (Connection myConn = DriverManager.getConnection(dbUrl, user, pass);
				CallableStatement myStmt = myConn.prepareCall("{call increase_salaries_for_department(?, ?)}");) {
			
			// Set the parameters
			myStmt.setString(1, departement);
			myStmt.setDouble(2, increaseAmount);

			// Call stored procedure
			System.out.println("\n\nCalling stored procedure.  increase_salaries_for_department('" + departement + "', " + increaseAmount + ")");
			myStmt.execute();
			System.out.println("Finished calling stored procedure");

			} catch (SQLIntegrityConstraintViolationException e) {
			    System.out.println("The person is already exist with this id");
			}catch (Exception exc) {
			    exc.printStackTrace();
			}	
	}

		
}
