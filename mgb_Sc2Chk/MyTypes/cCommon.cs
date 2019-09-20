// Версия 1.43  от 8 апреля 2019г. Процедуры и функции общего назначения
using	MyTypes				;
using	System.Runtime.InteropServices	;
using	int64	=	System.Int64	;
using	money	=	System.Decimal	;
using	datetime=	System.DateTime	;

namespace MyTypes
{
	public sealed class CCommon
	{
		private static	System.IO.FileAttributes 		FileAttr	;
		private static	object 					CodeClass	;
		private static	System.Reflection.Assembly 		CodeLoader	;
		private static	int 					CodeFlags	= (int)	System.Reflection.BindingFlags.Default | (int) System.Reflection.BindingFlags.InvokeMethod;
		private static	System.IO.StreamWriter			TextWriter	;
		private static	System.IO.StreamReader			TextReader	;
		private static	System.Random				Randomize	= new	System.Random();
		private static	System.Text.StringBuilder		StrBuilder	= new	System.Text.StringBuilder();
		private static	System.Windows.Forms.OpenFileDialog	OpenFileDlgObj	= new	System.Windows.Forms.OpenFileDialog();
		private static	System.Windows.Forms.SaveFileDialog	SaveFileDlgObj	= new	System.Windows.Forms.SaveFileDialog();
		private static	System.Runtime.Serialization.Formatters.Soap.SoapFormatter SoapFormatter = new System.Runtime.Serialization.Formatters.Soap.SoapFormatter();
		private	static	System.Windows.Forms.Form		MessageForm	= new	System.Windows.Forms.Form();
		private	static	System.Drawing.Font			MessageFont	= new	System.Drawing.Font("Lucida Console", 10, System.Drawing.FontStyle.Bold);
		private	static	System.Drawing.Graphics			MessageBrush	;
		private static readonly System.DateTime			START_DATE	= new	System.DateTime(1900, 1, 1);
		private static readonly char[]  CUTSTR_SEPARATORS = { System.Convert.ToChar(59) , System.Convert.ToChar(44) , System.Convert.ToChar(32) , System.Convert.ToChar(9) };

		[DllImport("kernel32.dll", EntryPoint = "Sleep", CharSet = CharSet.Auto)]
		internal static extern int Sleep(int dwMilliseconds) ;

		[DllImport("user32.dll", EntryPoint = "OemToCharA" , CharSet = CharSet.Auto)]
		internal static extern int OemToCharA( string lpszSrc , string lpszDst ) ;

		[DllImport("user32.dll", EntryPoint = "CharToOemA" , CharSet = CharSet.Auto)]
		internal static extern int CharToOemA( string lpszSrc , string lpszDst ) ;

		[DllImport("shell32.dll")]
		internal static extern int ShellExecute( int hWnd , string lpOperation , string lpFile , string lpParameters , string lpDirectory , int nShowCmd ) ;

		[DllImport("kernel32.dll", EntryPoint = "GetPrivateProfileStringA" , CharSet = CharSet.Auto)]
		internal static extern int GetPrivateProfileString( string Section , string Key , string DefaultStr , string GetStr , int nSize , string IniFile ) ;

		[DllImport("kernel32.dll", EntryPoint = "WritePrivateProfileStringA" , CharSet = CharSet.Auto)]
		internal static extern int WritePrivateProfileString( string Section , string Key , string PutStr , string IniFile ) ;

		[DllImport("user32.dll", EntryPoint="MessageBox")]
		public static extern int MessageBox(int hWnd, string strMessage, string strCaption, uint uiType);

		[DllImport("user32.dll")]
		static extern System.IntPtr GetClipboardData(uint uFormat);

		[DllImport("user32.dll")]
		static extern bool IsClipboardFormatAvailable(uint format);

		[DllImport("user32.dll", SetLastError = true)]
		static extern bool OpenClipboard( System.IntPtr hWndNewOwner);

		[DllImport("user32.dll", SetLastError = true)]
		static extern bool CloseClipboard();

		[DllImport("kernel32.dll")]
		static extern System.IntPtr GlobalLock( System.IntPtr hMem);

		[DllImport("kernel32.dll")]
		static extern bool GlobalUnlock( System.IntPtr hMem);

		[System.Runtime.InteropServices.DllImport("user32.dll")]
		public static extern bool ShowWindow( int hWnd, int nCmdShow );

		[System.Runtime.InteropServices.DllImport("user32.dll")]
		public static extern bool FlashWindow( int hWnd, int bInvert );

		public static short Abs(short S)
		{
			try {
				return System.Math.Abs(S);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
		}

		public static int Abs(int I)
		{
			try {
				return System.Math.Abs(I);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
		}

		public static long Abs(long L)
		{
			try {
				return System.Math.Abs(L);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
		}

		public static decimal Abs(decimal D)
		{
			try {
				return System.Math.Abs(D);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
		}

		public	static	string	AddSlash( string Src  )
		{
			if	( Src == null )
				Src	=	CAbc.EMPTY;
			Src	=	Src.Trim();
			if	( Src.Length == 0 )
				return	CAbc.SLASH ;
			if	( Src[ Src.Length - 1 ] == CAbc.SLASH[ 0 ] )
				return	Src;
			return	Src + CAbc.SLASH ;
		}

		public static bool AppendText(string FileName, object Obj, int CharSet )
		{
			if (Obj == null)
				return false;
			try {
				TextWriter = new System.IO.StreamWriter(FileName.Trim(), true, System.Text.Encoding.GetEncoding(CharSet));
				TextWriter.WriteLine(Obj.ToString());
				TextWriter.Close();
				TextWriter = null;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return FileExists(FileName.Trim());
		}

		public static short Ascii(char SourceChr)
		{
			try {
				return System.Convert.ToInt16(SourceChr);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
		}

		public static bool AttributeExists(object Obj, string AttributeName)
		{
			object[] Attrs;
			if ((Obj == null) || (AttributeName == null)) {
				return false;
			} else {
				Attrs = GetAttributes(Obj);
				foreach ( object Attr in Attrs) {
					if (Attr.GetType().Name.ToUpper().Equals(AttributeName.Trim().ToUpper() + "ATTRIBUTE")) {
						return true;
					}
				}
			}
			return false;
		}

		public static byte Avg(byte[] ObjectList)
		{
			if (ObjectList.Length == 0)
				return 0;
			if (Count(ObjectList) == 0)
				return 0;
			return (byte) ( Sum(ObjectList) / Count(ObjectList) );
		}

		public static short Avg(short[] ObjectList)
		{
			if (ObjectList.Length == 0)
				return 0;
			if (Count(ObjectList) == 0)
				return 0;
			return (short) ( Sum(ObjectList) / Count(ObjectList) );
		}

		public static int Avg(int[] ObjectList)
		{
			if (ObjectList.Length == 0)
				return 0;
			if (Count(ObjectList) == 0)
				return 0;
			return Sum(ObjectList) / Count(ObjectList);
		}

		public static long Avg(long[] ObjectList)
		{
			if (ObjectList.Length == 0)
				return 0;
			if (Count(ObjectList) == 0)
				return 0;
			return Sum(ObjectList) / Count(ObjectList);
		}

		public static decimal Avg(decimal[] ObjectList)
		{
			if (ObjectList.Length == 0)
				return 0;
			if (Count(ObjectList) == 0)
				return 0;
			return Sum(ObjectList) / Count(ObjectList);
		}

		public static string BinaryToString(byte[] SourceBin, int CharSet)
		{
			if (SourceBin == null)
				return null;
			try {
				return System.Text.Encoding.GetEncoding(CharSet).GetString(SourceBin);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
			return null;
		}

		public static decimal CCur(object SourceObj)
		{
			if (SourceObj == null)
				return 0;
			try {
				return System.Decimal.Parse(SourceObj.ToString().Trim().Replace(".",","));
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
		}

		public	static	string	Center( string St , int NewLen )
		{
			if	( ( NewLen < 1 ) || ( St == null ) )
				return	CAbc.EMPTY;
			St	=	St.Trim();
			int	OldLen	=	St.Length;
			if	( OldLen > NewLen )
				return	Replicate( "#" , NewLen );
			int	Delta	=	( NewLen - OldLen ) >> 1 ;
			return	( ( Delta > 0 ) ? Space( Delta ) : "" )	 + St
			+	( ( ( NewLen - OldLen - Delta ) > 0 ) ? Space( NewLen - OldLen - Delta ) : "" );
		}

		public static int CharIndex(string ToFind, string FullStr, int StartIndex )
		{
			if ((ToFind == null) || (FullStr == null))
				return 0;
			return StrPos(ToFind, FullStr, StartIndex);
		}

		public static int CharIndex(string ToFind, string FullStr )
		{
			if ((ToFind == null) || (FullStr == null))
				return 0;
			return StrPos(ToFind, FullStr, 0);
		}

		public static string CharToOem(string WinStr)
		{
			if (WinStr == null)
				return null;
			string DosStr = (string) WinStr.Clone();
			try {
				CharToOemA(WinStr, DosStr);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				DosStr = null;
			}
			return DosStr;
		}

		public static char Chr(object Obj)
		{
			if (Obj == null)
				Obj="";
			try {
				return System.Convert.ToChar(Obj);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return	System.Convert.ToChar(" ");
			}
		}

		public static int CInt(object SourceObj)
		{
			return	CInt32(SourceObj);
		}

		public static int CInt32(object SourceObj)
		{
			if (SourceObj == null)
				return 0;
			try {
				return System.Int32.Parse(SourceObj.ToString());
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
		}

		public static long CInt64(object SourceObj)
		{
			if (SourceObj == null)
				return 0;
			try {
				return System.Int64.Parse(SourceObj.ToString().Trim());
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
		}

		public static double CDbl(object SourceObj)
		{
			if (SourceObj == null)
				return 0;
			try {
				return System.Double.Parse(SourceObj.ToString().Trim().Replace(".",",") );
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
		}

		public static float CFlt(object SourceObj)
		{
			return	(float) CDbl( SourceObj );
		}

		public static long CLng(object SourceObj)
		{
			return	CInt64(SourceObj);
		}

		public static int Clock()
		{
			return GetTime(Now());
		}

		public static object Coalesce(params object[] ObjectList)
		{
			if (ObjectList.Length > 0) {
				foreach ( object Obj in ObjectList) {
					if (!(Obj == null))
						return Obj;
				}
			}
			return null;
		}

		public static bool CopyArray(System.Array SourceArray, System.Array DestArray)
		{
			if (SourceArray == null)	return false;
			if (DestArray == null)		return false;
			if (SourceArray.Length == 0)	return false;
			if (DestArray.Length == 0)	return false;
			try {
				if( SourceArray.Length > DestArray.Length)
					System.Array.Copy( SourceArray, DestArray, DestArray.Length);
				else
					System.Array.Copy( SourceArray, DestArray, SourceArray.Length);
			}
			catch (System.Exception Excpt)
			{
				Err.Add(Excpt);
				return	false;
			}
			return	true;
		}

		public static bool CopyFile(string SourceName, string TargetName)
		{
			if ((SourceName == null) || (TargetName == null))
				return false;
			System.IO.FileInfo FileInfo;
			try {
				FileInfo = new System.IO.FileInfo(SourceName);
				if (FileInfo == null)
					return false;
				FileInfo.CopyTo(TargetName);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public static int Count(string SourceStr)
		{
			return Len(SourceStr);
		}

		public static int Count(params object[] ObjectList)
		{
			return Len(ObjectList);
		}

		public static int Count(System.Collections.IList[] ObjectList)
		{
			return Len(ObjectList);
		}

		public static int Count(System.Collections.IEnumerable ObjectList)
		{
			return Len(ObjectList);
		}

		public static int Count(System.Array Arr)
		{
			return Len(Arr);
		}

		public static bool CreateText(string FileName, object Obj, int CharSet)
		{
			if ((FileName == null) || (Obj == null))
				return false;
			if (Obj == null)
				return false;
			if (FileExists(FileName.Trim())) {
				if (!DeleteFile(FileName.Trim()))
					return false;
			}
			return AppendText(FileName.Trim(), Obj, CharSet);
		}

		public static int CTime(object Obj)
		{
			if (Obj == null)
				return (int)0;
			try {
				return (int)System.Int32.Parse(Obj.ToString().Trim());
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return (int)0;
			}
		}

		public static int CtoD(string DateStr)
		{
			if (DateStr == null)
				return 0;
			string TmpStr = DateStr.Trim();
			if (TmpStr.Length != 8)
				return 0;
			TmpStr = TmpStr.Substring(6, 2) + "/" + TmpStr.Substring(4, 2) + "/" + TmpStr.Substring(0, 4);
			return GetDate(TmpStr);
		}

		public static decimal CutNum(ref string SourceStr)
		{
			if (IsEmpty(SourceStr))
				return 0;
			return Val(CutStr(ref SourceStr).Trim());
		}

		public static string CutStr(ref string SourceStr)
		{
			if (IsEmpty(SourceStr))
				return CAbc.EMPTY;
			int I;
			string[] TargetStr = SourceStr.Trim().Split(CUTSTR_SEPARATORS, 2);
			SourceStr = "";
			for (I = LBound(TargetStr,0); I <= UBound(TargetStr,0); I++) {
				if (I > 0)
					SourceStr = SourceStr + TargetStr[I];
			}
			return TargetStr[0];
		}

		public static int DataLength(object SourceObj)
		{
			if (SourceObj == null)
				return 0;
			try {
				if (SourceObj.ToString().Trim() == "")
					return 0;
				return SourceObj.ToString().TrimEnd().Length;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
		}

		public static long DateToLong(System.DateTime TimeDate)
		{
			return TimeDate.Ticks;
		}

		public static int Day(System.DateTime TimeDate)
		{
			return TimeDate.Day;
		}

		public static int Day(int IntDate)
		{
			if (IntDate == 0)
				IntDate = Today();
			return Day(GetDateTime(IntDate));
		}

		public static int DayOfWeek(System.DateTime TimeDate)
		{
			try {
				return (int)TimeDate.DayOfWeek;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
		}

		public static int DayOfWeek(int IntDate)
		{
			if (IntDate == 0)
				IntDate = Today();
			try {
				return DayOfWeek(GetDateTime(IntDate));
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
		}

		public static bool DeleteDir(string DirName)
		{
			if (DirName == null)
				return false;
			try {
				System.IO.Directory.Delete(DirName);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			if (DirExists(DirName))
				return false;
			else
				return true;
		}

		public static bool DeleteFile(string FileName)
		{
			if (FileName == null)
				return false;
			try {
				System.IO.File.Delete(FileName);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			if (FileExists(FileName))
				return false;
			else
				return true;
		}

		public	static	void	DeleteOldTempDirs( string FileMask , int LastDay )
		//	example	DeleteOldTempDirs("??????" , CCommon.Today() - 1 );
		{
			if	( FileMask == null )
				return;
			string[] Directories	=	GetDirList( GetTempDir() + "\\" + FileMask.Trim() );
				if	( Directories == null )
					return;
				if	( Directories.Length == 0 )
					return;
			string[] Files	=	null;
			foreach	( string Directory in Directories ) {
				if	( GetDate( CCommon.GetFileTime( Directory.Trim() ) ) < LastDay ) {
					Files	=	GetDirList( Directory.Trim() + "\\*"  );
					if	( Files != null )
						if	( Files.Length > 0 )
								continue;
					Files	=	CCommon.GetFileList( Directory.Trim() + "\\*"  );
					if	( Files != null )
						if	( Files.Length > 0 )
							foreach	( string File in Files )
								if	( ! CCommon.DeleteFile( File ) )
									break;
					CCommon.DeleteDir( Directory );
				}
			}
		}

		public static bool DirExists(string DirName)
		{
			if (DirName == null)
				return false;
			bool IsExists = false;
			try {
				IsExists = System.IO.Directory.Exists(DirName);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				IsExists = false;
			}
			return IsExists;
		}

		public static string DtoC(int DateInt)
		{
			System.DateTime TmpDate = GetDateTime(DateInt);
			string TmpStr = TmpDate.ToString().Trim();
			TmpStr = TmpStr.Substring(6, 4) + TmpStr.Substring(3, 2) + TmpStr.Substring(0, 2);
			return TmpStr;
		}

		public static bool FileExists(string FileName)
		{
			if (FileName == null)
				return false;
			bool IsExists = false;
			try {	IsExists = System.IO.File.Exists(FileName);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				IsExists = false;
			}
			return IsExists;
		}

		//   Из строки убирает символ /  и все что после него
		public	static	string	FixDocNum( string SrcStr ) {
			if	( SrcStr == null )
				return	"";
			if	( SrcStr.IndexOf("/")>0 )
				return	SrcStr.Substring(0, SrcStr.IndexOf("/") ) ;
			return	SrcStr;
		}

		public	static	string	FixUkrI( string SrcStr ) {
			if	( SrcStr == null ) {
				return	"";
			}
			return	SrcStr.Replace(CAbc.BIG_UKR_I,"I").Replace(CAbc.SMALL_UKR_I,"i");
		}

		public	static	string	Format( System.IFormattable Value , string Maska )
		{	if	( Maska == null )
				return	"";
			if	( Maska.Length == 0 )
				return	"";
			string	Result	=	CCommon.Replicate("#",Maska.Length);
			try{	Result	=	Value.ToString( Maska.Replace(",",".").Replace("#","0").Replace("<"," ") , null );
			}catch	(System.Exception Excpt) {
				Err.Add(Excpt);
				Result	=	CCommon.Replicate("#",Maska.Length);;
			}
			if	( Maska.IndexOf(".") > 0 )
				return	Result.Replace( "," , "." );
			return	Result;
		}

		public static object GetAttribute(object Obj, string AttributeName)
		{
			object[] Attrs;
			if (Obj == null) {
				return null;
			} else {
				Attrs = GetAttributes(Obj);
				foreach ( object Attr in Attrs) {
					if (Attr.GetType().Name.ToUpper().Equals( AttributeName.ToUpper() + "ATTRIBUTE")) {
						return Attr;
					}
				}
			}
			return null;
		}

		public static object[] GetAttributes(object Obj)
		{
			if (Obj == null) {
				return null;
			} else {
				return Obj.GetType().GetCustomAttributes(true);
			}
		}

		public static string GetClipboardText() {
			const uint CF_UNICODETEXT = 13;
            		if	( !IsClipboardFormatAvailable(CF_UNICODETEXT) )
				return CAbc.EMPTY;
			if ( !OpenClipboard( System.IntPtr.Zero) )
				return CAbc.EMPTY;
			string	data = CAbc.EMPTY;
			System.IntPtr	hGlobal	=	GetClipboardData( CF_UNICODETEXT );
			if	( hGlobal != System.IntPtr.Zero ) {
				System.IntPtr lpwcstr = GlobalLock( hGlobal );
				if	(lpwcstr != System.IntPtr.Zero) {
					data	=	Marshal.PtrToStringUni( lpwcstr );
					GlobalUnlock( lpwcstr );
				}
			}
			CloseClipboard();
			return data;
		}

		public	static	string	GetCodeByMoniker( string Moniker ) {
			string	Appendix	=	"";
			if	( Moniker == null ) {
				return	"";
			}
			Moniker	=	Moniker.Trim();
			if	( Moniker.Length < 5 ) {
				return	Moniker;
			}
			int	Index	=	Moniker.IndexOf(".");
			if	(  Index > 0 ) {
				Appendix=Moniker.Substring( Index );
				Moniker=Moniker.Substring( 0 , Index );
			}
			if	( Moniker.Length > 5 ) {
				return	Moniker.Substring(0,4) + Right( Moniker.Substring(5,Moniker.Length-5).Trim() , 9 ) + Moniker.Substring(4,1)  + Appendix;
			}
			else	{
				return	Moniker.Substring(0,4) + Right( Moniker.Substring(4) , 10 ) + Appendix ;
			}
		}

		public static string GetCommandLine()
		{
			string CmdLine;
			try {
				CmdLine = System.Environment.CommandLine;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				CmdLine = null;
			}
			return CmdLine;
		}

		public static string[] GetCommandLines()
		{
			string[] CmdLines;
			try {
				CmdLines = System.Environment.GetCommandLineArgs();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				CmdLines = null;
			}
			return CmdLines;
		}

		public static string GetConfigValue(string ConfigKey)
		{
			if (ConfigKey == null)
				return null;
			try {
				System.Configuration.AppSettingsReader Environment = new System.Configuration.AppSettingsReader();
				return	(string)IsNull(Environment.GetValue(ConfigKey, typeof(System.String)), "");
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return "";
			}
		}

		public static string GetCurDir()
		{
			string CurDir;
			try {
				CurDir = System.IO.Directory.GetCurrentDirectory();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				CurDir = null;
			}
			return CurDir;
		}

		public static int GetDate(System.DateTime TimeDate)
		{
			try {
				return (int) ( Trunc( TimeDate.ToOADate() ) - Trunc( START_DATE.ToOADate() ) );
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
		}

		public static int GetDate(string DateStr)
		{
			if (DateStr == null)
				return 0;
			System.DateTime TmpDate;
			try {
				TmpDate = System.DateTime.Parse( DateStr.Replace(".", "/").Trim() );
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
			return GetDate(TmpDate);
		}

		public static System.DateTime GetDateTime(int IntDate)
		{
			if (IntDate == 0)
				return START_DATE;
			try {
				IntDate = IntDate + (int)START_DATE.ToOADate();
				return (System.DateTime)System.DateTime.FromOADate((double)IntDate);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return START_DATE;
			}
		}

		public static System.DateTime GetDateTime(string StrDate)
		{
			if (StrDate == null)
				return START_DATE;
			if (StrDate.Trim().Length == 0)
				return START_DATE;
			try {
				return System.DateTime.Parse(StrDate.Replace(".", "/").Trim());
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
			return START_DATE;
		}

		public static string[] GetDirList(string FileMask)
		{
			string[] DirList;
			try {
				DirList = System.IO.Directory.GetDirectories(GetDirName(FileMask), GetFileName(FileMask));
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				DirList = null;
			}
			return DirList;
		}

		public static string GetDirName(string FullFileName)
		{
			if (FullFileName == null)
				return null;
			string DirName;
			if (FullFileName == null) {
				return null;
			} else {
				try {
					DirName = System.IO.Path.GetDirectoryName(FullFileName.Trim());
				} catch (System.Exception Excpt) {
					Err.Add(Excpt);
					DirName = null;
				}
			}
			return DirName.Trim();
		}

		public static string GetDomainName()
		{
			string DomainName;
			try {
				DomainName = System.Environment.UserDomainName;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				DomainName = null;
			}
			return DomainName;
		}

		public static string[] GetDrives()
		{
			string[] Drives;
			try {
				Drives = System.IO.Directory.GetLogicalDrives();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				Drives = null;
			}
			return Drives;
		}

		public static string GetEnvStr(string VariableName)
		{
			if (VariableName == null)
				return null;
			string EnvStr;
			if (!(VariableName == null)) {
				try {
					EnvStr = System.Environment.GetEnvironmentVariable(VariableName);
				} catch (System.Exception Excpt) {
					Err.Add(Excpt);
					EnvStr = null;
				}
			} else {
				EnvStr = null;
			}
			return EnvStr;
		}

		public static string GetExtension(string FullFileName)
		{
			string Extension;
			if (FullFileName == null) {
				return null;
			} else {
				try {
					Extension = System.IO.Path.GetExtension(FullFileName);
				} catch (System.Exception Excpt) {
					Err.Add(Excpt);
					Extension = null;
				}
			}
			return Extension;
		}

		public static string[] GetFileList(string FileMask)
		{
			string[] FileList;
			string	DirName		=	GetDirName(FileMask);
			DirName	= IsEmpty( DirName ) ? GetCurDir() : DirName ;
			try {
				FileList = System.IO.Directory.GetFiles( DirName , GetFileName(FileMask));
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				FileList = CAbc.EMPTY_STRING_LIST;
			}
			return FileList;
		}

		public static string GetFileName(string FullFileName)
		{
			string FileName;
			if (FullFileName == null) {
				return null;
			} else {
				try {
					FileName = System.IO.Path.GetFileName(FullFileName.Trim());
				} catch (System.Exception Excpt) {
					Err.Add(Excpt);
					FileName = null;
				}
			}
			return FileName.Trim();
		}

		public	static long GetFileSize(string FileName)
		{
			if (FileName == null)
				return 0;
			System.IO.FileInfo FileInfo;
			try {
				FileInfo = new System.IO.FileInfo(FileName);
				if (FileInfo == null) {
					return 0;
				} else {
					return FileInfo.Length;
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
			return 0;
		}

		public	static System.DateTime GetFileTime( string FileName )
		{	System.DateTime	Result	= new	System.DateTime(1900, 1, 1);
			if ( FileName == null )
				return Result;
			FileName=FileName.Trim();
			if ( FileName == "" )
				return Result;
			System.IO.FileInfo FileInfo;
			try {
				FileInfo = new System.IO.FileInfo(FileName);
				if (FileInfo == null) {
					return Result;
				} else {
					return FileInfo.LastWriteTime ;
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return Result;
			}
			return Result;
		}

		public static string GetHostName()
		{
			string HostName;
			try {
				HostName = System.Environment.MachineName;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				HostName = null;
			}
			return HostName;
		}

		public static string GetIniKey(string FileName, string Section, string Key)
		{
			if ((FileName == null) || (Section == null) || (Key == null))
				return null;
			string GetStr = RepStr("*", 255);
			try {
				GetPrivateProfileString(Section, Key, "", GetStr, GetStr.Length, FileName);
				if (GetStr.IndexOf(Chr(0)) > 0) {
					return GetStr.Substring(0, GetStr.IndexOf(Chr(0)));
				} else {
					return GetStr.Trim();
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return null;
			}
		}

		public	static	string	GetMonikerByCode( string Code ) {
			if	( Code == null ) {
				return	"";
			}
			Code	=	Code.Trim();
			if	( Code.Length < 14 ) {
				return	"";
			}
			if	( Code.Length > 14 ) {
				return	Code.Substring(0,4) + Code.Substring(13,1) + Code.Substring(4,9).Trim() + Code.Substring(14);
			}
			else	{
				return	Code.Substring(0,4) + Code.Substring(13,1) + Code.Substring(4,9).Trim();
			}
		}

		public static string GetNetVersion()
		{
			return System.Environment.Version.ToString();
		}

		public static string GetOsVersion()
		{
			return System.Environment.OSVersion.Version.ToString();
		}

		public static System.Data.SqlClient.SqlDataReader GetRecordSet(string ConnectionString, string CommandText)
		{
			System.Data.SqlClient.SqlConnection Conn = new System.Data.SqlClient.SqlConnection();
			System.Data.SqlClient.SqlCommand Comm;
			Conn.ConnectionString = ConnectionString;
			try {
				Conn.Open();
			} catch (System.Exception Excpt) {
				Err.Add( Excpt );
				return null;
			}
			try {
				Comm = Conn.CreateCommand();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				Conn.Close();
				return null;
			}
			Comm.CommandText = CommandText;
			try {
				return (System.Data.SqlClient.SqlDataReader) Comm.ExecuteReader();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				Conn.Close();
				return null;
			}
		}

		public static string GetShortName(string FullFileName)
		{
			string ShortName;
			if (FullFileName == null) {
				return null;
			} else {
				try {
					ShortName = System.IO.Path.GetFileNameWithoutExtension(FullFileName);
				} catch (System.Exception Excpt) {
					Err.Add(Excpt);
					ShortName = null;
				}
			}
			return ShortName;
		}

		public static string GetSysDir()
		{
			string SysDir;
			try {
				SysDir = System.Environment.SystemDirectory.Trim();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				SysDir = null;
			}
			return SysDir;
		}

		public static string GetTaskDir()
		{
			try {
				return GetDirName(GetTaskName()).Trim();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return null;
			}
		}

		public static string GetTaskName()
		{
			try {
				return System.Reflection.Assembly.GetExecutingAssembly().Location.Trim();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return null;
			}
		}

		public static string GetTempDir()
		{
			string TempDir = null;
			try {
				TempDir = System.Environment.GetEnvironmentVariable("TEMP").Trim();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				TempDir = null;
			}
			if (!(TempDir == null) & TempDir != "" & DirExists(TempDir)) {
				return TempDir;
			} else {
				try {
					TempDir = System.Environment.GetEnvironmentVariable("TMP").Trim();
				} catch (System.Exception Excpt) {
					Err.Add(Excpt);
					TempDir = null;
				}
			}
			if (!(TempDir == null) & TempDir != "" & DirExists(TempDir)) {
				return TempDir;
			} else {
				try {
					TempDir = System.IO.Path.GetTempPath().Trim();
				} catch (System.Exception Excpt) {
					Err.Add(Excpt);
					TempDir = null;
				}
			}
			if (!(TempDir == null) & TempDir != "" & DirExists(TempDir)) {
				return TempDir;
			} else {
				return null;
			}
		}

		public static string GetTempName()
		{
			string TempFile;
			try {
				TempFile = System.IO.Path.GetTempFileName().Trim();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				TempFile = null;
			}
			return TempFile;
		}

		public static int GetTime(string TimeStr)
		{
			System.DateTime TmpDate;
			try {
				TmpDate = System.DateTime.Parse(TimeStr.Trim());
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
			return (int)(((TmpDate.Hour * 60) + TmpDate.Minute) * 60 + TmpDate.Second);
		}

		public static int GetTime(System.DateTime TimeTime)
		{
			return GetTime(TimeTime.ToString().Trim());
		}

		public static string GetUserDir()
		{
			string UserDir = null;
			try {
				UserDir = System.Environment.GetEnvironmentVariable("UserProfile");
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				UserDir = null;
			}
			if (!(UserDir == null) & UserDir != "" & DirExists(UserDir)) {
				return UserDir;
			} else {
				return null;
			}
		}

		public static string GetUserName()
		{
			string UserName;
			try {
				UserName = System.Environment.UserName.Trim();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				UserName = null;
			}
			return UserName;
		}

		public static string GetWinDir()
		{
			string WinDir = null;
			try {
				WinDir = System.Environment.GetEnvironmentVariable("windir").Trim();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				WinDir = null;
			}
			if (!(WinDir == null) & WinDir != "" & DirExists(WinDir)) {
				return WinDir;
			} else {
				try {
					WinDir = System.Environment.GetEnvironmentVariable("SystemRoot");
				} catch (System.Exception Excpt) {
					Err.Add(Excpt);
					WinDir = null;
				}
			}
			if (!(WinDir == null) & WinDir != "" & DirExists(WinDir)) {
				return WinDir;
			} else {
				return null;
			}
		}

		public static int Hour(int IntTime)
		{
			if (IntTime == 0)
				IntTime = Clock();
			return (int) (IntTime / 3600) ;
		}

		public static int Hour(System.DateTime TimeDate)
		{
			return	Hour( GetTime(TimeDate) );
		}

		public static string Input()
		{
			return System.Console.ReadLine();
		}

		public static string InputBox(string Title, string PromptText, ref string Value)
		{
			System.Windows.Forms.Form form = new System.Windows.Forms.Form();
			System.Windows.Forms.Label label = new System.Windows.Forms.Label();
			System.Windows.Forms.TextBox textBox = new System.Windows.Forms.TextBox();
			System.Windows.Forms.Button buttonOk = new System.Windows.Forms.Button();
			System.Windows.Forms.Button buttonCancel = new System.Windows.Forms.Button();
			form.Text = Title;
			label.Text = PromptText;
			textBox.Text = Value;
			buttonOk.Text = "OK";
			buttonCancel.Text = "Cancel";
			buttonOk.DialogResult = System.Windows.Forms.DialogResult.OK;
			buttonCancel.DialogResult = System.Windows.Forms.DialogResult.Cancel;
			label.SetBounds(9, 20, 372, 13);
			textBox.SetBounds(12, 36, 372, 20);
			buttonOk.SetBounds(228, 72, 75, 23);
			buttonCancel.SetBounds(309, 72, 75, 23);
			label.AutoSize = true;
			textBox.Anchor = textBox.Anchor | System.Windows.Forms.AnchorStyles.Right;
			buttonOk.Anchor = System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Right;
			buttonCancel.Anchor = System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Right;
			form.ClientSize = new System.Drawing.Size(396, 107);
			form.Controls.AddRange(new System.Windows.Forms.Control[] { label, textBox, buttonOk, buttonCancel });
			form.ClientSize = new System.Drawing.Size(System.Math.Max(300, label.Right + 10), form.ClientSize.Height);
			form.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedDialog;
			form.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
			form.MinimizeBox = false;
			form.MaximizeBox = false;
			form.AcceptButton = buttonOk;
			form.CancelButton = buttonCancel;
			if ( form.ShowDialog() == System.Windows.Forms.DialogResult.OK )
				return textBox.Text;
			else
				return null;
		}

		public static bool IsDigit( char Ch )
		{
			return System.Char.IsDigit(Ch);
		}

		public static bool IsDigit( string St )
		{	if	( St == null )
				return	false	;
			if	( St.Trim() == CAbc.EMPTY )
				return	false	;
			bool	Result	=	true	;
			int	I	=	0	;
			string	Tmps	=	St.Trim()	;
			while	( I < CCommon.Len( Tmps ) )
			{
				if	( ! System.Char.IsDigit(Tmps,I) )
					Result	=	false;
				I++;
			}
			return	Result;
		}

		public static bool IsDigitEx( string St )
		{	if	( St == null )
				return	false	;
			St	=	St.Replace(",","").Replace(".","").Trim();
			if	( St == CAbc.EMPTY )
				return	false	;
			bool	Result	=	true	;
			int	I	=	0	;
			string	Tmps	=	St.Trim()	;
			while	( I < CCommon.Len( Tmps ) )
			{
				if	( ! System.Char.IsDigit(Tmps,I) )
					Result	=	false;
				I++;
			}
			return	Result;
		}

		public static bool IsEmpty(object Obj)
		{
			if (Obj == null) {
				return true;
			} else if (Obj.ToString().Trim() == "") {
				return true;
			} else {
				return false;
			}
		}

		public static bool IsFileHidden(string FileName)
		{
			if (FileName == null)
				return false;
			bool HaveError = false;
			try {
				FileAttr = System.IO.File.GetAttributes(FileName);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				HaveError = true;
			}
			if (HaveError)
				return false;
			if ((FileAttr & System.IO.FileAttributes.Hidden) == System.IO.FileAttributes.Hidden) {
				return true;
			} else {
				return false;
			}
		}

		public static bool IsFileReadOnly(string FileName)
		{
			if (FileName == null)
				return false;
			bool HaveError = false;
			try {
				FileAttr = System.IO.File.GetAttributes(FileName);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				HaveError = true;
			}
			if (HaveError)
				return false;
			if ((FileAttr & System.IO.FileAttributes.ReadOnly) == System.IO.FileAttributes.ReadOnly) {
				return true;
			} else {
				return false;
			}
		}

		public static bool IsFileSystem(string FileName)
		{
			if (FileName == null)
				return false;
			bool HaveError = false;
			try {
				FileAttr = System.IO.File.GetAttributes(FileName);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				HaveError = true;
			}
			if (HaveError)
				return false;
			if ((FileAttr & System.IO.FileAttributes.System) == System.IO.FileAttributes.System) {
				return true;
			} else {
				return false;
			}
		}

		public static bool IsLetter(char Ch)
		{
			return System.Char.IsLetter(Ch);
		}

		public static bool IsLetter(string St)
		{
			if (St == null)
				return false;
			if	( St.Trim() == CAbc.EMPTY )
				return	false	;
			return System.Char.IsLetter(St, 0);
		}

		public static bool IsMatch(string SourceStr, string PatternStr)
		{
			if ((SourceStr == null) || (PatternStr == null))
				return false;
			if ((SourceStr.Length == 0) || (PatternStr.Length == 0))
				return false;
			try {
				return System.Text.RegularExpressions.Regex.IsMatch(SourceStr, PatternStr, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
		}

		public static object IsNull(object Obj, object Default)
		{
			if (Obj == null)
				return Default;
			else
				return Obj;
		}

		public static bool IsNumeric(string St)
		{
			if (St == null)
				return false;
			foreach ( char Ch in St.Trim().ToCharArray()) {
				if ( ( Ch!=',' ) && ( Ch!='.') )
					if (!System.Char.IsNumber(Ch))
						return false;
			}
			return true;
		}

		public static int LBound(System.Array Arr, int Rank)
		{
			if (Arr == null)
				return 0;
			try {
				return Arr.GetLowerBound(Rank);
			} catch (System.Exception Excpt) {
				return 0;
			}
		}

		public static string Left(string St, int StrSize)
		{
			if (St == null)
				return CAbc.EMPTY;
			if (StrSize == 0)
				return CAbc.EMPTY;
			return St.PadRight(StrSize).Substring(0,StrSize);
		}

		public static int Len(string SourceStr)
		{
			if (SourceStr == null)
				return 0;
			return SourceStr.Length;
		}

		public static int Len(params object[] ObjectList)
		{
			if (ObjectList == null)
				return 0;
			return ObjectList.Length;
		}

		public static int Len(System.Collections.IList[] ObjectList)
		{
			if (ObjectList == null)
				return 0;
			return ObjectList.Length;
		}

		public static int Len(System.Array Arr, int Dimension)
		{
			if (Arr == null)
				return 0;
			try {
				return Arr.GetLength(Dimension);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
		}

		public static int Len(System.Collections.IEnumerable ObjectList)
		{
			if (ObjectList == null)
				return 0;
			int Result = 0;
			try {
				foreach ( object Obj in ObjectList) {
					Result = Result + 1;
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
			return Result;
		}

		public static string LoadText(string FileName, int CharSet )
		{
			if (FileName == null)
				return null;
			if (FileName.Length == 0)
				return null;
			try {
				TextReader = new System.IO.StreamReader(FileName, System.Text.Encoding.GetEncoding(CharSet));
				string Result = TextReader.ReadToEnd();
				TextReader.Close();
				return Result;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return null;
			}
		}

		public static System.DateTime LongToDate(long Ticks)
		{
			System.DateTime ResDate = new System.DateTime(1, 1, 1);
			return ResDate.AddTicks(Ticks);
		}

		public static string Lower(object Obj)
		{
			if ((Obj == null)) {
				return "";
			} else {
				return Obj.ToString().ToLower();
			}
		}

		public static string LTrim(string St)
		{
			if (St == null)
				return null;
			return St.TrimStart();
		}

		public static byte Max(byte[] ObjectList)
		{
			if (ObjectList == null)
				return 0;
			if (ObjectList.Length == 0)
				return 0;
			byte Result = ObjectList[0];
			try {
				foreach ( byte Obj in ObjectList) {
					if (Obj > Result)
						Result = Obj;
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
			return Result;
		}

		public static short Max(short[] ObjectList)
		{
			if (ObjectList == null)
				return 0;
			if (ObjectList.Length == 0)
				return 0;
			short Result = ObjectList[0];
			try {
				foreach ( short Obj in ObjectList) {
					if (Obj > Result)
						Result = Obj;
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
			return Result;
		}

		public static int Max(int[] ObjectList)
		{
			if (ObjectList == null)
				return 0;
			if (ObjectList.Length == 0)
				return 0;
			int Result = ObjectList[0];
			try {
				foreach ( int Obj in ObjectList) {
					if (Obj > Result)
						Result = Obj;
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
			return Result;
		}

		public static long Max(long[] ObjectList)
		{
			if (ObjectList == null)
				return 0;
			if (ObjectList.Length == 0)
				return 0;
			long Result = ObjectList[0];
			try {
				foreach ( long Obj in ObjectList) {
					if (Obj > Result)
						Result = Obj;
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
			return Result;
		}

		public static decimal Max(decimal[] ObjectList)
		{
			if (ObjectList == null)
				return 0;
			if (ObjectList.Length == 0)
				return 0;
			decimal Result = ObjectList[0];
			try {
				foreach ( decimal Obj in ObjectList) {
					if (Obj > Result)
						Result = Obj;
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
			return Result;
		}

		public static int MsgBox(string InfoText, string TitleText, int Flags)
		{
			if (InfoText == null)
				return CAbc.MB_NONE;
			if (TitleText == null)
				return CAbc.MB_NONE;
			System.Windows.Forms.MessageBoxButtons Buttons = System.Windows.Forms.MessageBoxButtons.OK;
			System.Windows.Forms.MessageBoxIcon BoxStyle = System.Windows.Forms.MessageBoxIcon.None;
			System.Windows.Forms.DialogResult BoxResult = System.Windows.Forms.DialogResult.None;
			if ((Flags & CAbc.MB_OK) == CAbc.MB_OK)
				Buttons = System.Windows.Forms.MessageBoxButtons.OK;
			if ((Flags & CAbc.MB_YESNO) == CAbc.MB_YESNO)
				Buttons = System.Windows.Forms.MessageBoxButtons.YesNo;
			if ((Flags & CAbc.MB_OKCANCEL) == CAbc.MB_OKCANCEL)
				Buttons = System.Windows.Forms.MessageBoxButtons.OKCancel;
			if ((Flags & CAbc.MB_RETRYCANCEL) == CAbc.MB_RETRYCANCEL)
				Buttons = System.Windows.Forms.MessageBoxButtons.RetryCancel;
			if ((Flags & CAbc.MB_YESNOCANCEL) == CAbc.MB_YESNOCANCEL)
				Buttons = System.Windows.Forms.MessageBoxButtons.YesNoCancel;
			if ((Flags & CAbc.MB_ABORTRETRYIGNORE) == CAbc.MB_ABORTRETRYIGNORE)
				Buttons = System.Windows.Forms.MessageBoxButtons.AbortRetryIgnore;
			if ((Flags & CAbc.MB_HAND) == CAbc.MB_HAND)
				BoxStyle = System.Windows.Forms.MessageBoxIcon.Hand;
			if ((Flags & CAbc.MB_QUESTION) == CAbc.MB_QUESTION)
				BoxStyle = System.Windows.Forms.MessageBoxIcon.Question;
			if ((Flags & CAbc.MB_EXCLAMATION) == CAbc.MB_EXCLAMATION)
				BoxStyle = System.Windows.Forms.MessageBoxIcon.Exclamation;
			if ((Flags & CAbc.MB_ASTERISK) == CAbc.MB_ASTERISK)
				BoxStyle = System.Windows.Forms.MessageBoxIcon.Asterisk;
			if ((Flags & CAbc.MB_STOP) == CAbc.MB_STOP)
				BoxStyle = System.Windows.Forms.MessageBoxIcon.Stop;
			if ((Flags & CAbc.MB_ERROR) == CAbc.MB_ERROR)
				BoxStyle = System.Windows.Forms.MessageBoxIcon.Error;
			if ((Flags & CAbc.MB_WARNING) == CAbc.MB_WARNING)
				BoxStyle = System.Windows.Forms.MessageBoxIcon.Warning;
			if ((Flags & CAbc.MB_INFORMATION) == CAbc.MB_INFORMATION)
				BoxStyle = System.Windows.Forms.MessageBoxIcon.Information;
			try {
				BoxResult = System.Windows.Forms.MessageBox.Show(InfoText, TitleText, Buttons, BoxStyle);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return	CAbc.MB_NONE;
			}
			if (BoxResult == System.Windows.Forms.DialogResult.None)
				return	CAbc.MB_NONE;
			if (BoxResult == System.Windows.Forms.DialogResult.OK)
				return	CAbc.MB_OK;
			if (BoxResult == System.Windows.Forms.DialogResult.Cancel)
				return	CAbc.MB_CANCEL;
			if (BoxResult == System.Windows.Forms.DialogResult.Yes)
				return	CAbc.MB_YES;
			if (BoxResult == System.Windows.Forms.DialogResult.No)
				return	CAbc.MB_NO;
			if (BoxResult == System.Windows.Forms.DialogResult.Retry)
				return	CAbc.MB_RETRY;
			if (BoxResult == System.Windows.Forms.DialogResult.Abort)
				return	CAbc.MB_ABORT;
			if (BoxResult == System.Windows.Forms.DialogResult.Ignore)
				return	CAbc.MB_IGNORE;
			return	CAbc.MB_NONE;
		}

		public static byte Min(byte[] ObjectList)
		{
			if (ObjectList == null)
				return 0;
			if (ObjectList.Length == 0)
				return 0;
			byte Result = ObjectList[0];
			try {
				foreach ( byte Obj in ObjectList) {
					if (Obj < Result)
						Result = Obj;
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
			return Result;
		}

		public static short Min(short[] ObjectList)
		{
			if (ObjectList == null)
				return 0;
			if (ObjectList.Length == 0)
				return 0;
			short Result = ObjectList[0];
			try {
				foreach ( short Obj in ObjectList) {
					if (Obj < Result)
						Result = Obj;
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
			return Result;
		}

		public static int Min(int[] ObjectList)
		{
			if (ObjectList == null)
				return 0;
			if (ObjectList.Length == 0)
				return 0;
			int Result = ObjectList[0];
			try {
				foreach ( int Obj in ObjectList) {
					if (Obj < Result)
						Result = Obj;
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
			return Result;
		}

		public static long Min(long[] ObjectList)
		{
			if (ObjectList == null)
				return 0;
			if (ObjectList.Length == 0)
				return 0;
			long Result = ObjectList[0];
			try {
				foreach ( long Obj in ObjectList) {
					if (Obj < Result)
						Result = Obj;
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
			return Result;
		}

		public static decimal Min(decimal[] ObjectList)
		{
			if (ObjectList == null)
				return 0;
			if (ObjectList.Length == 0)
				return 0;
			decimal Result = ObjectList[0];
			try {
				foreach ( decimal Obj in ObjectList) {
					if (Obj < Result)
						Result = Obj;
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
			return Result;
		}

		public static int Minute(int IntTime)
		{
			if (IntTime == 0)
				IntTime = Clock();
			return (int) ( IntTime / 60 - (Hour(IntTime) * 60) );
		}

		public static int Minute(System.DateTime TimeDate)
		{
			return Minute(GetTime(TimeDate));
		}

		public static bool MkDir(string DirName)
		{
			try {
				System.IO.Directory.CreateDirectory(DirName);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public static int Month(System.DateTime TimeDate)
		{
			return TimeDate.Month;
		}

		public static int Month(int IntDate)
		{
			if (IntDate == 0)
				IntDate = Today();
			return Month(GetDateTime(IntDate));
		}

		public static bool MoveDir(string SourceName, string TargetName)
		{
			if ((SourceName == null) || (TargetName == null))
				return false;
			if ((SourceName.Trim() == "") || (TargetName.Trim() == ""))
				return false;
			try {
				System.IO.Directory.Move(SourceName, TargetName);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public static bool MoveFile(string SourceName, string TargetName)
		{
			if ((SourceName == null) || (TargetName == null))
				return false;
			if ((SourceName.Trim() == "") || (TargetName.Trim() == ""))
				return false;
			try {
				System.IO.File.Move(SourceName, TargetName);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public static System.DateTime Now()
		{
			return System.DateTime.Now;
		}

		public static string ObjToSoap(object Obj)
		{
			if (Obj == null)
				return null;
			System.IO.MemoryStream SoapStream = new System.IO.MemoryStream();
			try {
				SoapFormatter.Serialize(SoapStream, Obj);
				SoapStream.Flush();
				SoapStream.Seek(0, System.IO.SeekOrigin.Begin);
				return new System.IO.StreamReader(SoapStream).ReadToEnd();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return null;
			}
		}

		public static string OemToChar(string DosStr)
		{
			if (DosStr == null)
				return null;
			string WinStr = (string)DosStr.Clone();
			try {
				OemToCharA(DosStr, WinStr);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return	"";
			}
			return WinStr;
		}

		// выбор одного файла
		public static string OpenFileBox(string TitleText, string StartDirectory, string Filter)
		{
			// exaplle	OpenFileBox ( "","","Text Files (*.txt)|*.txt|All Files (*.*)|*.*" )
			if (Filter == null)
				return "";
			if (TitleText == null)
				return "";
			if (StartDirectory == null)
				return "";
			OpenFileDlgObj.Filter = Filter;
			OpenFileDlgObj.Title = TitleText;
			OpenFileDlgObj.InitialDirectory = StartDirectory;
			OpenFileDlgObj.RestoreDirectory = true;
			OpenFileDlgObj.FilterIndex = 1;
			OpenFileDlgObj.Multiselect = false;
			try {
				if (OpenFileDlgObj.ShowDialog() == System.Windows.Forms.DialogResult.OK) {
					return OpenFileDlgObj.FileName;
				} else {
					return null;
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return null;
			}
			return null;
		}

		// выбор нескольких файлов
		public static string[] OpenFilesBox(string TitleText, string StartDirectory, string Filter)
		{
			// exaplle	OpenFileBox ( "","","Text Files (*.txt)|*.txt|All Files (*.*)|*.*" )
			if (Filter == null)
				return null;
			if (TitleText == null)
				return null;
			if (StartDirectory == null)
				return null;
			OpenFileDlgObj.Filter = Filter;
			OpenFileDlgObj.Title = TitleText;
			OpenFileDlgObj.InitialDirectory = StartDirectory;
			OpenFileDlgObj.RestoreDirectory = true;
			OpenFileDlgObj.FilterIndex = 1;
			OpenFileDlgObj.Multiselect = true;
			try {
				if (OpenFileDlgObj.ShowDialog() == System.Windows.Forms.DialogResult.OK) {
					return OpenFileDlgObj.FileNames;
				} else {
					return null;
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return null;
			}
			return null;
		}

		public static int Ord( char Ch ) {
			return	System.Convert.ToInt32( Ch );
		}

		public static int ParamCount()
		{
			if ( CAbc.ParamStr == null )
				return 0;
			return CAbc.ParamStr.Length;
		}

		public static void Print(string AnyStr)
		{
			if (AnyStr == null)
				return;
			try {
				System.Console.WriteLine(AnyStr);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
		}

		public static void Print(System.Collections.IEnumerable ObjectList)
		{
			if (ObjectList == null)
				return;
			try {
				foreach ( object Obj in ObjectList) {
					if (!(Obj == null)) {
						System.Console.WriteLine(Obj.ToString());
					}
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
		}

		public static void Print(params object[] ObjectList)
		{
			if (ObjectList == null)
				return;
			if (ObjectList.Length == 0)
				return;
			try {
				foreach ( object Obj in ObjectList) {
					if (!(Obj == null)) {
						System.Console.WriteLine(Obj.ToString());
					}
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
		}

		public static string Progress(System.Decimal Percent, byte Count)
		{
			if (Percent > 1)
				Percent = 1;
			if (Percent < 0)
				Percent = 0;
			int I;
			StrBuilder.Length = 0;
			for (I = 1; I <= (int)(Percent * Count); I++) {
				StrBuilder.Append(Chr(9608));
			}
			for (I = (int)(Percent * Count) + 1; I <= Count; I++) {
				StrBuilder.Append(Chr(9618));
			}
			return StrBuilder.ToString();
		}

		public static void Quit(int ExitCode)
		{
			try {
				System.Environment.Exit(ExitCode);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
		}

		public static void RaisError(string MsgText)
		{
			if (MsgText == null)
				return;
			try {
				throw new System.ApplicationException(MsgText);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
		}

		public static string ReCode(string SourceStr, int FromCharset, int ToCharset)
		{
			if (SourceStr == null)
				return null;
			if (SourceStr.Trim().Length == 0)
				return SourceStr;
			try {
				return BinaryToString(StringToBinary(SourceStr, FromCharset), ToCharset);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
			return null;
		}

		public static string Replace(string St, string FromSt, string ToSt)
		{
			if ((St == null) || (FromSt == null) || (ToSt == null))
				return null;
			return St.Replace(FromSt, ToSt);
		}

		public static string Replicate(string SourceStr, int Count)
		{
			return RepStr(SourceStr, Count);
		}

		public static string RepStr(string SourceStr, int Count)
		{
			if ((Count == 0) || (SourceStr == null))
				return System.String.Empty;
			StrBuilder.Length = 0;
			while (Count > 0) {
				StrBuilder.Append(SourceStr);
				Count = Count - 1;
			}
			return StrBuilder.ToString();
		}

		public static string Right(string St, int StrSize)
		{
			if (St == null)
				return	CAbc.EMPTY ;
			if (StrSize == 0)
				return	CAbc.EMPTY ;
			string	Result = St.PadLeft(StrSize) ;
			return	Result.Substring(Result.Length-StrSize,StrSize) ;
		}

		public static bool RmDir(string DirName)
		{
			try {
				System.IO.Directory.Delete(DirName);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public static int Rnd(int MaxValue)
		{
			if (MaxValue < 0)
				return 0;
			try {
				return Randomize.Next(0, MaxValue);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
		}

		public static decimal Round(decimal DecVal, int Precision)
		{
			decimal ResVal;
			if (DecVal == 0)
				return 0;
			if ( ( Precision > 28 ) || (Precision < 0 ) ) {
				ResVal = 0;
			} else {
				try {
					ResVal = System.Math.Round(DecVal, Precision);
				} catch (System.Exception Excpt) {
					Err.Add(Excpt);
					ResVal = 0;
				}
			}
			return ResVal;
		}

		public static double Round(double DecVal, int Precision)
		{
			double ResVal;
			if (DecVal == 0)
				return 0;
			if ( ( Precision > 28 ) || (Precision < 0 ) ) {
				ResVal = 0;
			} else {
				try {
					ResVal = System.Math.Round(DecVal, Precision);
				} catch (System.Exception Excpt) {
					Err.Add(Excpt);
					ResVal = 0;
				}
			}
			return ResVal;
		}

		public static string RTrim(string St)
		{
			if (St == null)
				return null;
			return St.TrimEnd();
		}

		public static bool Run(string ProgName, string Arguments)
		{
			if (ProgName == null)
				return false;
			bool Result = true;
			try {
				System.Diagnostics.ProcessStartInfo StartInfo = new	System.Diagnostics.ProcessStartInfo(ProgName);
				StartInfo.WindowStyle	=	System.Diagnostics.ProcessWindowStyle.Normal;
				StartInfo.Arguments	= 	Arguments ;
				StartInfo.CreateNoWindow=	false ;
				StartInfo.UseShellExecute=	false ;
				System.Diagnostics.Process	Process	= new	System.Diagnostics.Process();
				Process			=	System.Diagnostics.Process.Start( StartInfo  );
				Process.WaitForExit();
				Process.Close();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				Result = false;
			}
			return Result;
		}

		public static bool RunAssembly(string FileName, string ClassName)
		{
			if (FileName == null)
				return false;
			try {
				System.Reflection.Assembly CodeLoader = System.Reflection.Assembly.LoadFrom(FileName.Trim());
				object CodeClass = CodeLoader.CreateInstance(ClassName, true);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public static string SaveFileBox(string TitleText, string StartDirectory, string Filter)
		{
			// exaplle	SaveFileBox ( "","","Text Files (*.txt)|*.txt|All Files (*.*)|*.*" )
			if (Filter == null)
				return "";
			if (TitleText == null)
				return "";
			if (StartDirectory == null)
				return "";
			SaveFileDlgObj.Filter = Filter;
			SaveFileDlgObj.Title = TitleText;
			SaveFileDlgObj.InitialDirectory = StartDirectory;
			SaveFileDlgObj.RestoreDirectory = true;
			SaveFileDlgObj.FilterIndex = 1;
			try {
				if (SaveFileDlgObj.ShowDialog() == System.Windows.Forms.DialogResult.OK) {
					return SaveFileDlgObj.FileName;
				} else {
					return null;
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return null;
			}
			return null;
		}

		public static bool SaveText(string FileName, object Obj, int CharSet )
		{
			if (Obj == null)
				return false;
			try {
				TextWriter = new System.IO.StreamWriter(FileName.Trim(), false, System.Text.Encoding.GetEncoding(CharSet));
				TextWriter.WriteLine(Obj.ToString());
				TextWriter.Close();
				TextWriter = null;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return FileExists(FileName.Trim());
		}

		public static int Second(int IntTime)
		{
			if (IntTime == 0)
				IntTime = Clock();
			return (int) ( IntTime - (Hour(IntTime) * 3600) - (Minute(IntTime) * 60) );
		}

		public static int Second(System.DateTime TimeDate)
		{
			return	Second( GetTime(TimeDate) ) ;
		}

		public static bool SetCurDir(string DirName)
		{
			try {
				System.IO.Directory.SetCurrentDirectory(DirName);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public static bool SetFileAttr(string FileName, bool SetReadOnly)
		{
			if (FileName == null)
				return false;
			try {
				FileAttr = System.IO.File.GetAttributes(FileName);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			FileAttr = FileAttr | System.IO.FileAttributes.ReadOnly;
			if (SetReadOnly) {
			} else {
				FileAttr = FileAttr ^ System.IO.FileAttributes.ReadOnly;
			}
			try {
				System.IO.File.SetAttributes(FileName, FileAttr);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		// 	https://rosettacode.org/wiki/Truncate_a_file#C.23
		public	static	bool SetFileSize( string FileName , long NewSize ) {
			if	( FileName == null)
				return false;
			if	( ! FileExists( FileName ) )
				return	false;
			try {
				System.IO.FileStream	FileStream ;
				FileStream = new	System.IO.FileStream( FileName , System.IO.FileMode.Open, System.IO.FileAccess.Write) ;
				FileStream.SetLength( NewSize );
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public static bool SetIniKey(string FileName, string Section, string Key, string wStr)
		{
			if ((FileName == null) || (Section == null) || (Key == null))
				return false;
			try {
				WritePrivateProfileString(Section, Key, wStr, FileName);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public static bool Shell(string FileName, string Parameters, string Action, int WndStyle)
		{
			if (FileName == null)
				return false;
			try {
				ShellExecute(0, Action.Trim(), GetFileName(FileName.Trim()), Parameters.Trim(), GetDirName(FileName.Trim()), WndStyle);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public static void ShowMessage(string MesssageText)
		{
			if (MessageBrush == null) {
				MessageForm.Text = "";
				MessageForm.ClientSize = new System.Drawing.Size(346, 99);
				MessageForm.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedToolWindow;
				MessageForm.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
				MessageForm.ShowInTaskbar = false;
				MessageForm.MinimizeBox = false;
				MessageForm.MaximizeBox = false;
				MessageForm.UseWaitCursor = false;
				MessageForm.TopLevel = true;
				MessageBrush = MessageForm.CreateGraphics();
			}
			if (MesssageText == null) {
				MessageForm.Visible = false;
				return;
			} else {
				if (MesssageText.Trim() == "") {
					MessageForm.Visible = false;
					return;
				}
			}
			if (MessageForm.Visible) {
			} else {
				MessageForm.Visible = true;
			}
			MessageBrush.Clear(MessageForm.BackColor);
			MessageBrush.DrawString(MesssageText, MessageFont, System.Drawing.Brushes.Black, 0, 0);
		}

		public static object SoapToObj(string Soap)
		{
			if (Soap == null)
				return null;
			System.IO.MemoryStream SoapStream = new System.IO.MemoryStream();
			try {
				System.IO.StreamWriter SoapWriter = new System.IO.StreamWriter(SoapStream);
				SoapWriter.Write(Soap);
				SoapWriter.Flush();
				SoapStream.Flush();
				SoapStream.Seek(0, System.IO.SeekOrigin.Begin);
				return SoapFormatter.Deserialize(SoapStream);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return null;
			}
		}

		public static string Space(int Count)
		{
			return	Replicate(System.Convert.ToChar(32).ToString(),Count);
		}

		public static string[] Split(string SourceString, params char[] Separators)
		{
			if (SourceString == null) {
				string Tmps = "";
				return Tmps.Split();
			} else {
				if (Separators.Length == 0) {
					return SourceString.Split( CUTSTR_SEPARATORS );
				} else {
					return SourceString.Split(Separators, 10000);
				}
			}
		}

		public static decimal Sqr(object SomeValue)
		{
			try {
				return (decimal)System.Math.Sqrt((double)SomeValue);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return (decimal)0;
			}
		}

		public	static	int	StrDate_To_IntDate(string StrDate)
		{	if	(StrDate==null)
				return 0;
			if	(StrDate.Trim().Length!=8)
				return 0;
			int	D_Year	=	System.Int32.Parse(StrDate.Trim().Substring(0,4));
			int	D_Mon	=	System.Int32.Parse(StrDate.Trim().Substring(4,2));
			int	D_Day	=	System.Int32.Parse(StrDate.Trim().Substring(6,2));
			System.DateTime	StartDate	= new	System.DateTime(1900,01,01);
			System.DateTime	TimeDate	= new	System.DateTime(D_Year,D_Mon,D_Day);
			return	( (int)TimeDate.ToOADate() - (int)StartDate.ToOADate() );
		}

		public static byte[] StringToBinary(string SourceStr, int Charset)
		{
			if (SourceStr == null)
				return null;
			try {
				return System.Text.Encoding.GetEncoding(Charset).GetBytes(SourceStr);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
			return null;
		}

		private static string StrChk(string St, int StrSize)
		{
			string TmpStr;
			TmpStr = St;
			if (StrSize > 0) {
				if (TmpStr.Length > StrSize) {
					TmpStr = RepStr("#", StrSize);
				} else {
					TmpStr = Right(St, StrSize);
				}
			}
			return TmpStr;
		}

		public static string StrD(int DateInt, byte StrLen, byte StrSize)
		{
			System.DateTime TmpDate = GetDateTime(DateInt);
			string TmpStr = TmpDate.ToString().Trim();
			if (StrLen == 8) {
				TmpStr = TmpStr.Substring(0, 2) + "/" + TmpStr.Substring(3, 2) + "/" + TmpStr.Substring(8, 2);
			} else {
				TmpStr = TmpStr.Substring(0, 2) + "/" + TmpStr.Substring(3, 2) + "/" + TmpStr.Substring(6, 4);
			}
			return StrChk(TmpStr, StrSize);
		}

		public static string StrE(decimal Num, int StrSize)
		{
			return StrChk(System.String.Format("{0:E}", Num), StrSize);
		}

		public static string StrH(byte Num, int StrSize)
		{
			return StrChk(System.String.Format("{0:X}", Num), StrSize);
		}

		public static string StrH(int Num, int StrSize)
		{
			return StrChk(System.String.Format("{0:X}", Num), StrSize);
		}

		public static string StrH(long Num, int StrSize)
		{
			return StrChk(System.String.Format("{0:X}", Num), StrSize);
		}

		public static string StrI(byte Num, int StrSize)
		{
			return StrChk(System.String.Format("{0:D}", Num), StrSize);
		}

		public static string StrI(int Num, int StrSize)
		{
			return StrChk(System.String.Format("{0:D}", Num), StrSize);
		}

		public static string StrI(long Num, int StrSize)
		{
			return StrChk(System.String.Format("{0:D}", Num), StrSize);
		}

		public static string StrL(object Obj, byte StrSize)
		{
			if (Obj == null)
				return null;
			if (StrSize == 0) {
				return Obj.ToString();
			} else if (StrSize > Obj.ToString().Length) {
				return Left(Obj.ToString(), StrSize);
			} else {
				return Obj.ToString().Substring(0, StrSize);
			}
		}

		public static string StrM(decimal Num, int StrSize)
		{
			return StrChk( System.String.Format("{0:N}", Num).Replace(",",".").Replace( Chr(160).ToString() , "`" ) , StrSize);
		}

		public static string StrN(decimal Num, int StrSize)
		{
			return StrChk( System.String.Format("{0:F}", Num).Replace(",",".") , StrSize);
		}

		public static int StrPos(string ToFind, string FullStr, int StartIndex)
		{
			if ((ToFind == null) || (FullStr == null))
				return -1;
			if (StartIndex > FullStr.Length)
				return -1;
			return FullStr.IndexOf(ToFind, StartIndex);
		}

		public static string StrR(object Obj, byte StrSize)
		{
			if (Obj == null)
				return null;
			if (StrSize == 0) {
				return Obj.ToString();
			} else if (StrSize > Obj.ToString().Length) {
				return Right(Obj.ToString(), StrSize);
			} else {
				return Obj.ToString().Substring(0, StrSize);
			}
		}

		public static string StrT(int TimeInt, byte StrLen)
		{
			try {
				if (StrLen == 5) {
					return Hour(TimeInt).ToString("00") + ":" + Minute(TimeInt).ToString("00");
				} else {
					return Hour(TimeInt).ToString("00") + ":" + Minute(TimeInt).ToString("00") + ":" + Second(TimeInt).ToString("00");
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
			return "00:00";
		}

		public static string StrX(long Num, byte MathBase, int StrSize)
		{
			long	Act;
			int	Ost;
			if  ( (MathBase < 2) || (MathBase > 36) )
				return "";
			StrBuilder.Length = 0;
			Num = Abs(Num);
			if (Num == 0)
				StrBuilder.Append("0");
			while (Num > 0) {
				Act = (long) ( Num / MathBase ) ;
				Ost = (int) ( Num - (Act * MathBase) );
				Num = Act;
				if (Ost < 10) {
					StrBuilder.Insert(0, Chr(Ost + 48));
				} else {
					StrBuilder.Insert(0, Chr(Ost + 55));
				}
			}
			return StrChk(StrBuilder.ToString(), StrSize);
		}

		public static string StrY(byte Num, int StrSize)
		{
			return StrX((long)Num, 32, StrSize);
		}

		public static string StrY(short Num, int StrSize)
		{
			return StrX((long)Num, 32, StrSize);
		}

		public static string StrY(int Num, int StrSize)
		{
			return StrX((long)Num, 32, StrSize);
		}

		public static string StrY(long Num, int StrSize)
		{
			return StrX(Num, 32, StrSize);
		}

		public static string StrZ(byte Num, int StrSize)
		{
			return StrX((long)Num, 36, StrSize);
		}

		public static string StrZ(short Num, int StrSize)
		{
			return StrX((long)Num, 36, StrSize);
		}

		public static string StrZ(int Num, int StrSize)
		{
			return StrX((long)Num, 36, StrSize);
		}

		public static string StrZ(long Num, int StrSize)
		{
			return StrX(Num, 36, StrSize);
		}

		public static string SubStr(string SourceStr, int StartPos, int StopPos)
		{
			if	( SourceStr == null )
				return	CAbc.EMPTY;
			if	(	( StopPos < StartPos)
				||	( StartPos < 0 )
				||	( StartPos >= SourceStr.Length )
				||	( IsEmpty(SourceStr) )
				)
				return	CAbc.EMPTY;
			try {
				if	( SourceStr.Length > StopPos )
					return	SourceStr.Substring( StartPos , StopPos - StartPos + 1);
				else
					return	SourceStr.Substring( StartPos );
			} catch ( System.Exception Excpt ) {
				Err.Add(Excpt);
				return CAbc.EMPTY;
			}
		}

		public static string Substring(string SourceStr, int StartPos, int NewLen)
		{
			if	( SourceStr == null )
				return	CAbc.EMPTY;
			if	(	( StartPos < 0 )
				||	( StartPos >= SourceStr.Length )
				||	( IsEmpty(SourceStr) )
				)
				return	CAbc.EMPTY;
			try {
				if	( SourceStr.Length > ( StartPos + NewLen - 1 ) )
					return SourceStr.Substring( StartPos , NewLen );
				else
					return SourceStr.Substring( StartPos );
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return null;
			}
		}

		public static string Substring(object SourceObj, int StartPos, int NewLen)
		{
			if (SourceObj == null)
				return null;
			return Substring(SourceObj.ToString(), StartPos, NewLen);
		}

		public static byte Sum(params byte[] ObjectList)
		{
			if (ObjectList.Length == 0)
				return 0;
			byte Result = 0;
			try {
				foreach ( byte Obj in ObjectList) {
					Result = (byte) ( Result + Obj );
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
			return Result;
		}

		public static short Sum(params short[] ObjectList)
		{
			if (ObjectList.Length == 0)
				return 0;
			short Result = 0;
			try {
				foreach ( short Obj in ObjectList) {
					Result = (short) ( Result + Obj );
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
			return Result;
		}

		public static int Sum(params int[] ObjectList)
		{
			if (ObjectList.Length == 0)
				return 0;
			int Result = 0;
			try {
				foreach ( int Obj in ObjectList) {
					Result = Result + Obj;
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
			return Result;
		}

		public static long Sum(params long[] ObjectList)
		{
			if (ObjectList.Length == 0)
				return 0;
			long Result = 0;
			try {
				foreach ( long Obj in ObjectList) {
					Result = Result + Obj;
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
			return Result;
		}

		public static decimal Sum(params decimal[] ObjectList)
		{
			if (ObjectList.Length == 0)
				return 0;
			decimal Result = 0;
			try {
				foreach ( decimal Obj in ObjectList) {
					Result = Result + Obj;
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
			return Result;
		}

		public static string SwapStr(string St, string FromSt, string ToSt)
		{
			if ((St == null) || (FromSt == null) || (ToSt == null))
				return null;
			try {
				return St.Replace(FromSt, ToSt);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return null;
			}
		}

		public static int Today()
		{
			return GetDate(Now());
		}

		public static string Trim(object Obj)
		{
			if (Obj == null)
				return "";
			return Obj.ToString().Trim();
		}

		public static string TrimQuotes(object Obj)
		{
			if (Obj == null)
				return "";
			return Obj.ToString().Trim().Replace("'","`");
		}

		public static decimal Trunc(decimal DecVal)
		{
			decimal ResVal;
			if (DecVal == 0)
				return 0;
			try {
				ResVal = System.Math.Truncate(DecVal);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				ResVal = 0;
			}
			return ResVal;
		}

		public static double Trunc(double DecVal)
		{
			double ResVal;
			if (DecVal == 0)
				return 0;
			try {
				ResVal = System.Math.Truncate(DecVal);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				ResVal = 0;
			}
			return ResVal;
		}

		public static int UBound(System.Array Arr, int Rank)
		{
			if (Arr == null)
				return 0;
			try {
				return Arr.GetUpperBound(Rank);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
		}

		public static string Upper(object Obj)
		{
			if ((Obj == null)) {
				return "";
			} else {
				return Obj.ToString().ToUpper();
			}
		}

		public static decimal Val(string SourceObj)
		{
			if (SourceObj == null)
				return 0;
			try {
				return System.Decimal.Parse(SourceObj.ToString().Replace(".", ","));
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
		}

		public static void Wait(int MilliSeconds)
		{
			try {
				System.Threading.Thread.Sleep(MilliSeconds);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
		}

		public static void Write(System.Collections.IEnumerable ObjectList)
		{
			if (ObjectList == null)
				return;
			try {
				foreach ( object Obj in ObjectList) {
					if (!(Obj == null)) {
						System.Console.Write(Obj.ToString());
					}
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
		}

		public static void Write(params object[] ObjectList)
		{
			if (ObjectList == null)
				return;
			if (ObjectList.Length == 0)
				return;
			try {
				foreach ( object Obj in ObjectList) {
					if (!(Obj == null)) {
						System.Console.Write(Obj.ToString());
					}
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
		}

		public static int Year(System.DateTime TimeDate)
		{
			return TimeDate.Year;
		}

		public static int Year(int IntDate)
		{
			return Year(GetDateTime(IntDate));
		}

	}

}