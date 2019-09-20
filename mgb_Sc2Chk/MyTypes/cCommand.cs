// Версия 1.07  от 11 октября 2016 г.  - Классы для работы с MsSql - сервером
using MyTypes;

namespace MyTypes
{
	//- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	public class CConnection
	{
		private System.Data.SqlClient.SqlConnection Conn;
		CConnection()
		{
			Conn = new System.Data.SqlClient.SqlConnection();
			Conn.InfoMessage += new System.Data.SqlClient.SqlInfoMessageEventHandler(Err.OnInfoMessage);
		}

		public CConnection(string ConnectionString)
		{
			if (ConnectionString == null) {
				Conn = null;
				return;
			}
			Conn = new System.Data.SqlClient.SqlConnection();
			Conn.InfoMessage += new System.Data.SqlClient.SqlInfoMessageEventHandler(Err.OnInfoMessage);
			if (Conn == null) {
				return;
			}
			Conn.ConnectionString = ConnectionString;
			try {
				Conn.Open();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				Close();
			}
		}

		public void Close()
		{
			if (Conn == null)
				return;
			try {
				Conn.Close();
				Conn = null;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
		}

		public System.Data.SqlClient.SqlCommand CreateCommand()
		{
			if (Conn == null)
				return null;
			try {
				return Conn.CreateCommand();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return null;
			}
		}

		public CCommand GetCommand()
		{
			CCommand Res = new CCommand(this);
			return Res;
		}

		public string ConnectionString {
			get { return Conn.ConnectionString; }
			set { Conn.ConnectionString = value; }
		}

		public bool IsOpen()
		{
			if (Conn == null)
				return false;
			if (Conn.State == System.Data.ConnectionState.Broken | Conn.State == System.Data.ConnectionState.Closed) {
				return false;
			} else {
				return true;
			}
		}

		public bool Open(string ServerName , string DatabaseName , string UserName , string Password )
		{
			if (IsOpen())
				Close();
			Conn = new System.Data.SqlClient.SqlConnection();
			if ( ( ServerName == null ) || ( ServerName.Trim() == "")  )
			{
				Conn.ConnectionString = "SERVER=(local)";
			} 
			else
			{
				Conn.ConnectionString = "SERVER=" + ServerName.Trim().ToUpper();
			}
			if ( (DatabaseName == null) || ( DatabaseName.Trim() == "" ) ) 
			{
			} 
			else 
			{
				Conn.ConnectionString = Conn.ConnectionString + ";DATABASE=" + DatabaseName.Trim().ToUpper();
			}
			if ( (UserName == null) || (UserName.Trim() == "") )
			{
				Conn.ConnectionString = Conn.ConnectionString + ";Integrated Security=TRUE;" ;
			}
			else 
			{
				Conn.ConnectionString = Conn.ConnectionString + ";UID=" + UserName.Trim();
				Conn.ConnectionString = Conn.ConnectionString + ";PWD=" + Password.Trim();
			}
			Conn.ConnectionString = Conn.ConnectionString + ";";
			try {
				Conn.Open();
			} catch (System.Exception Excpt) 
			{
				Err.Add(Excpt);
				return false;
			}
			return IsOpen();
		}
	}

	//- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	public class CCommand
	{
		private System.Data.SqlClient.SqlCommand Comm;

		public	CCommand(CConnection ConnObj)
		{
			Comm = null;
			if (ConnObj == null) {
			} else if (ConnObj.IsOpen()) {
				try {
					Comm = ConnObj.CreateCommand();
				} catch (System.Exception Excpt) {
					Err.Add(Excpt);
					Comm = null;
				}
			}
		}

		public void Close()
		{
			if	( Comm != null ) {
				Comm.Dispose();
				Comm = null;
			}
		}

		public bool IsOpen()
		{
			if (Comm == null)
				return false;
			else
				return true;
		}

		public int Timeout {
			get {
				if	( Comm == null ) 
					return 0;
				else
					return Comm.CommandTimeout;
			}
			set {
				if (Comm != null) 
					Comm.CommandTimeout = value;
			}
		}

		public bool Execute(string CommandText)
		{
			if (CommandText == null)
				return false;
			if (Comm == null)
				return false;
			Comm.CommandText = CommandText;
			try {
				Comm.ExecuteNonQuery();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt , CAbc.CRLF + " >> " + CommandText);
				return false;
			}
			return true;
		}

		public object GetScalar(string CommandText)
		{
			if (CommandText == null)
				return null;
			if (Comm == null)
				return null;
			Comm.CommandText = CommandText;
			try {
				return Comm.ExecuteScalar();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt , CAbc.CRLF + " >> " + CommandText);
				return null;
			}
			return null;
		}

		public System.Xml.XmlReader GetXml(string CommandText)
		{
			if (CommandText == null)
				return null;
			if (Comm == null)
				return null;
			Comm.CommandText = CommandText;
			try {
				return Comm.ExecuteXmlReader();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt , CAbc.CRLF + " >> " + CommandText);
				return null;
			}
		}

		public System.Data.SqlClient.SqlDataReader GetDataReader(string CommandText)
		{
			if (CommandText == null)
				return null;
			if (Comm == null)
				return null;
			Comm.CommandText = CommandText;
			try {
				return Comm.ExecuteReader();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt , CAbc.CRLF + " >> " + CommandText);
				return null;
			}
		}
	}

	//- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	public class CRecordSet
	{

		public System.Data.SqlClient.SqlDataReader DataReader; // !!!
		private CCommand Command;

		public	CRecordSet(CConnection Connection)
		{
			DataReader	=	null;
			Command		= new	CCommand(Connection);
			Command.Timeout	=	299;
		}

		public int FieldCount()
		{
			if (IsEmpty())
				return 0;
			return DataReader.FieldCount;
		}

		public bool Read()
		{
			if (DataReader == null)
				return false;
			return DataReader.Read();
		}

		public	string	GetName( int Index )
		{
			if (DataReader == null)
				return CAbc.EMPTY;
			try {
				return DataReader.GetName( Index );
				
			}	catch	( System.Exception Excpt ) {
					Err.Add(Excpt);
					return CAbc.EMPTY;
			}
			
		}

		public bool NextRecordSet()
		{
			if (DataReader == null)
				return false;
			return	DataReader.NextResult();
		}

		public string this[string Index] {
			get {
				try {
					return DataReader[Index].ToString();
				} catch (System.Exception Excpt) {
					Err.Add(Excpt);
					return CAbc.EMPTY;
				}									
			}
		}

		public string this[int Index] {
			get {
				if (DataReader.IsDBNull(Index) ) {
					return CAbc.EMPTY ;
				} else {
					try {
						return DataReader[Index].ToString();
					} catch (System.Exception Excpt) {
						Err.Add(Excpt);
						return CAbc.EMPTY;
					}					
				}
			}
		}

		public bool IsEmpty()
		{
			if	( (Command == null) || (DataReader == null ) ) 
				return true;
			if (DataReader.HasRows)
				return false;
			else
				return true;
		}

		public void Close()
		{
			if	( DataReader != null ) {
				DataReader.Close();
				DataReader = null;
			}
			if	( Command != null) {
				Command.Close();
				Command = null;
			}
		}

		public int Timeout {
			get {
				if	( Command == null ) 
					return 0;
				else
					return Command.Timeout;
			}
			set {
				if	( Command != null )
					Command.Timeout = value;
			}
		}

		public bool Open(string CommandText)
		{
			if	(Command == null)
				return	false;
			if	(!Command.IsOpen())
				return false;
			if	( DataReader != null) { 
				DataReader.Close();
				DataReader = null;
			}
			try {
				DataReader = Command.GetDataReader(CommandText);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt , CAbc.CRLF + " >> " + CommandText);
				return false;
			}
			if (DataReader == null)
				return false;
			return	true;
		}
	}
}