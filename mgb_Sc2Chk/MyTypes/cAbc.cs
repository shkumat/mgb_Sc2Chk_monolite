// Версия 1.18  от 24 сентября 2018 г. - Первичный модуль для других модулей
using	System.Runtime.InteropServices	;
using	int8	=	System.SByte;
using	int16	=	System.Int16;
using	int32	=	System.Int32;
using	int64	=	System.Int64;
using	uint8	=	System.Byte;
using	uint16	=	System.UInt16;
using	uint32	=	System.UInt32;
using	uint64	=	System.UInt64;
using	money	=	System.Decimal	;
using	datetime=	System.DateTime	;

namespace MyTypes
{
	public sealed class CAbc
	{
		public	const	int	CHARSET_DOS	=	866						;
		public	const	int	CHARSET_OEM	=	866						;
		public	const	int	CHARSET_UTF16	=	1200						;
		public	const	int	CHARSET_UTF16LE	=	1200						;
		public	const	int	CHARSET_UTF16BE	=	1201						;
		public	const	int	CHARSET_WINDOWS	=	1251						;
		public	const	int	CHARSET_MAC	=	10007						;
		public	const	int	CHARSET_MAC_U	=	10017						;
		public	const	int	CHARSET_UTF32	=	12000						;
		public	const	int	CHARSET_KOI8	=	20866						;
		public	const	int	CHARSET_KOI8_U	=	21866						;
		public	const	int	CHARSET_UTF7	=	65000						;
		public	const	int	CHARSET_UTF8	=	65001						;

		// Для MessageBox --------------------------------------
		public	const	int	MB_Ok		=	1						;
		public	const	int	MB_Cancel	=	2						;
		public	const	int	MB_Abort	=	3						;
		public	const	int	MB_Retry	=	4						;
		public	const	int	MB_Ignore	=	5						;
		public	const	int	MB_Yes		=	6						;
		public	const	int	MB_No		=	7						;
		public	const	int	MB_OkCancel	=	1						;
		public	const	int	MB_AbortRetryIgnore =	2						;
		public	const	int	MB_YesNoCancel	=	3						;
		public	const	int	MB_YesNo	=	4						;
		public	const	int	MB_RetryCancel	=	5						;
		public	const	int	MB_Stop		=	16						;
		public	const	int	MB_Question	=	32						;
		public	const	int	MB_Warning	=	48						;
		public	const	int	MB_Information	=	64						;
		public	const	int	MB_SystemModal	=	4096						;
		public	const	int	MB_TaskModal	=	8192						;
		
		// Для MsgBox -----------------------------------------
		public	const	int	MB_NONE		=	0						;
		public	const	int	MB_OK		=	1						;
		public	const	int	MB_CANCEL	=	2						;
		public	const	int	MB_YES		=	4						;
		public	const	int	MB_NO		=	8						;
		public	const	int	MB_RETRY	=	16						;
		public	const	int	MB_ABORT	=	32						;
		public	const	int	MB_IGNORE	=	64						;
		public	const	int	MB_HAND		=	1 * 256						;
		public	const	int	MB_QUESTION	=	2 * 256						;
		public	const	int	MB_EXCLAMATION	=	4 * 256						;
		public	const	int	MB_ASTERISK	=	8 * 256						;
		public	const	int	MB_STOP		=	16 * 256					;
		public	const	int	MB_ERROR	=	32 * 256					;
		public	const	int	MB_WARNING	=	64 * 256					;
		public	const	int	MB_INFORMATION	=	128 * 256					;
		public	const	int	MB_OKCANCEL	=	MB_OK + MB_CANCEL				;
		public	const	int	MB_YESNO	=	MB_YES + MB_NO					;
		public	const	int	MB_RETRYCANCEL	=	MB_RETRY + MB_CANCEL				;
		public	const	int	MB_YESNOCANCEL	=	MB_YES + MB_NO + MB_CANCEL			;
		public	const	int	MB_ABORTRETRYIGNORE =	MB_ABORT + MB_RETRY + MB_IGNORE			;
		
		//	https://msdn.microsoft.com/en-us/library/windows/desktop/ms633548(v=vs.85).aspx
		public	const	int	SW_HIDE		=	0						; 
		public	const	int	SW_SHOWNORMAL	=	1						;
		public	const	int	SW_SHOWMINIMIZED=	2						;
		public	const	int	SW_SHOWMAXIMIZED=	3						;
		public	const	int	SW_SHOWNOACTIVATE=	4						;
		public	const	int	SW_SHOW		=	5						;
		public	const	int	SW_MINIMIZE	=	6						; 
		public	const	int	SW_SHOWMINNOACTIVE=	7						; 
		public	const	int	SW_SHOWNA	=	8						; 
		public	const	int	SW_RESTORE	=	9						;
		public	const	int	SW_SHOWDEFAULT	=	10						; 
		public	const	int	SW_FORCEMINIMIZE=	11						; 
		
		public	static	readonly string	EMPTY	=	System.String.Empty				;
		public	static	readonly string	TAB	=	System.Convert.ToChar(9).ToString()		;
		public	static	readonly string	CRLF	=	System.Environment.NewLine			;
		public	static	readonly string	SLASH	=	System.IO.Path.DirectorySeparatorChar.ToString();
		public	static	readonly string	QUOTE	=	System.Convert.ToChar(34).ToString()		;
		public	static	readonly string	LINE_FEED=	System.Convert.ToChar(10).ToString()		;
		public	static	readonly string	FORM_FEED=	System.Convert.ToChar(12).ToString()		;
		public	static	readonly string	CARRIAGE_RETURN=System.Convert.ToChar(13).ToString()		;
		public	static	readonly string	BIG_UKR_I=	System.Convert.ToChar(1030).ToString()		;
		public	static	readonly string	SMALL_UKR_I=	System.Convert.ToChar(1110).ToString()		;
		public	static	readonly string[] EMPTY_STRING_LIST =	{}					;
		public	static	readonly string[] ParamStr=	System.Environment.GetCommandLineArgs()		;
	}

	//---------------------------------------------------------
	public sealed class ApplicationInstance
	{

		[DllImport("User32.dll", EntryPoint = "ShowWindow", CharSet = CharSet.Auto)]
		internal static extern int ShowWindow(int hWnd, int nCmdShow);

		private static System.Diagnostics.Process CurrentPrc;
		private static System.Diagnostics.Process OtherPrc;

		static ApplicationInstance()
		{
			try {
				CurrentPrc = System.Diagnostics.Process.GetCurrentProcess();
			} catch (System.Exception Excpt) {
				CurrentPrc = null;
			}
		}

		public System.Diagnostics.Process GetCurrent {
			get { return CurrentPrc; }
		}

		public static void Raise()
		{
			if (OtherPrc == null) {
			} else if (OtherPrc.MainWindowHandle.Equals(System.IntPtr.Zero)) {
			} else {
				try {
					ShowWindow( (int) OtherPrc.MainWindowHandle, 1);
				} catch (System.Exception Excpt) {
					return;
				}
				return;
			}
			return;
		}

		public static bool Exists()
		{
			string ProcessName = CurrentPrc.MainModule.ModuleName.Trim().ToUpper();
			ProcessName = ProcessName.Substring(0, ProcessName.IndexOf(".EXE"));
			OtherPrc = null;
			try {
				foreach ( System.Diagnostics.Process AnyPrc in System.Diagnostics.Process.GetProcessesByName(ProcessName)) {
					if (AnyPrc.Id != CurrentPrc.Id & OtherPrc.MainModule.FileName == CurrentPrc.MainModule.FileName) {
						OtherPrc=AnyPrc;
						return true;
					}
				}
			} catch (System.Exception Excpt) {
				OtherPrc = null;
				return false;
			}
			OtherPrc = null;
			return false;
		}

	}

	//------------------------------------------------------
	public class EnumeratorByte : System.Collections.IEnumerator
	{

		public	byte[] Bytes;
		int	CurrentIndex = -1;

		void System.Collections.IEnumerator.Reset()
		{
			CurrentIndex = -1;
		}

		bool System.Collections.IEnumerator.MoveNext()
		{
			if (Bytes == null)
				return false;
			CurrentIndex += 1;
			return (CurrentIndex <= Bytes.GetUpperBound(0));
		}

		object System.Collections.IEnumerator.Current {
			get {
				if (Bytes == null) {
					return null;
				} else {
					return Bytes[CurrentIndex];
				}
			}
		}

	}

	//----------------------------------------------
	public class EnumeratorChar : System.Collections.IEnumerator
	{

		public System.Text.StringBuilder Chars;
		int	CurrentIndex = -1;

		void System.Collections.IEnumerator.Reset()
		{
			CurrentIndex = -1;
		}

		bool System.Collections.IEnumerator.MoveNext()
		{
			if (Chars == null)
				return false;
			CurrentIndex += 1;
			return (CurrentIndex < Chars.Length);
		}

		object System.Collections.IEnumerator.Current {
			get {
				if (Chars == null) {
					return null;
				} else {
					return Chars[CurrentIndex];
				}
			}
		}

	}

	//---------------------------------------------------------
	public class EnumeratorString : System.Collections.IEnumerator
	{

		public	string[] Strings;
		int	CurrentIndex = -1;

		void System.Collections.IEnumerator.Reset()
		{
			CurrentIndex = -1;
		}

		bool System.Collections.IEnumerator.MoveNext()
		{
			if (Strings == null)
				return false;
			CurrentIndex += 1;
			return (CurrentIndex <= Strings.GetUpperBound(0));
		}

		object System.Collections.IEnumerator.Current {
			get {
				if (Strings == null) {
					return null;
				} else {
					return Strings[CurrentIndex].Replace( System.Convert.ToChar(10).ToString() , "");
				}
			}
		}

	}

	//-----------------------------------------------------
	public sealed class Err
	{
		private static	string	ErrCode		=	""	;
		private static	string	ErrSource	=	""	;
		private static	string	ErrCallStack	=	""	;
		private	static	string	ErrDescription	=	""	;
		private	static	string	OutputFileName	=	null	;
		private static	System.IO.StreamWriter	Output		;

		public static string Code {
			get { return ErrCode; }
		}

		public static string Source {
			get { return ErrSource; }
		}

		public static string Description {
			get { return ErrDescription; }
		}

		public	static	void Debug()
		{
			System.Diagnostics.Debugger.Launch();
		}

		public static void Clear()
		{	ErrCode = "";
			ErrSource = "";
			ErrDescription = "";
		}

		public static void Add(System.Exception Excpt)
		{
			Clear();
			ErrCode = Excpt.GetType().ToString().Trim();
			ErrSource = Excpt.Source.Trim();
			ErrCallStack = Excpt.StackTrace.Trim();
			ErrDescription = Excpt.Message.Trim() + "     " ;
			Print(ToString());
		}

		public static void Add(System.Exception Excpt , string Description )
		{
			Clear();
			ErrCode = Excpt.GetType().ToString().Trim();
			ErrSource = Excpt.Source.Trim();
			ErrCallStack = Excpt.StackTrace.Trim();
			ErrDescription = Excpt.Message.Trim() + "     " + Description;
			Print(ToString());
		}

		public static new string ToString()
		{
			string Vb_Tab = System.Convert.ToChar(9).ToString()  ;
			string Vb_Cr_Lf = System.Environment.NewLine;
			if (ErrCode == "") {
				return ("");
			} else {
				return (Vb_Cr_Lf + System.DateTime.Now.ToString().Substring(0, 16) + " " + ErrCode.Replace(Vb_Cr_Lf, Vb_Cr_Lf + Vb_Tab) + " in `" + ErrSource.Replace(Vb_Cr_Lf, Vb_Cr_Lf + Vb_Tab ) + "`" + Vb_Cr_Lf + Vb_Tab + ErrDescription.Replace(Vb_Cr_Lf, Vb_Cr_Lf + Vb_Tab) + Vb_Cr_Lf + Vb_Tab + ErrCallStack.Replace(Vb_Cr_Lf, Vb_Cr_Lf + Vb_Tab) + Vb_Cr_Lf);
			}
		}


		public static void OnInfoMessage(object Sender, System.Data.SqlClient.SqlInfoMessageEventArgs Args)
		{
			string Vb_Tab = System.Convert.ToChar(9).ToString();
			string Vb_Cr_Lf = System.Environment.NewLine;
			foreach ( System.Data.SqlClient.SqlError SqlErr in Args.Errors) {
				Print(Vb_Cr_Lf + System.DateTime.Now.ToString().Substring(0, 16) + " . The error occured on server `" + SqlErr.Server.ToString().Trim() + "` : " + SqlErr.Source.ToString().Trim() + Vb_Cr_Lf + Vb_Tab + ", has received a severity " + SqlErr.Class.ToString().Trim() + ", state " + SqlErr.State + " ,error number " + SqlErr.Number.ToString().Trim() + " , on line " + SqlErr.LineNumber.ToString().Trim() + " of procedure `" + SqlErr.Procedure.ToString().Trim() + "`" + Vb_Cr_Lf +  Vb_Tab + SqlErr.Message.ToString().Trim().Replace(Vb_Cr_Lf, Vb_Cr_Lf + Vb_Tab) + Vb_Cr_Lf);
			}
		}

		public static void Print(string MsgStr)
		{	if	(MsgStr == null)
				return;
			if	( OutputFileName == null )
				return;
			try {
				if	( OutputFileName.Length == 0 )
                                {	System.Console.WriteLine(MsgStr);
				}
                                else
                                {	Output	= new System.IO.StreamWriter(OutputFileName.Trim(), true, System.Text.Encoding.GetEncoding(CAbc.CHARSET_WINDOWS));
					Output.WriteLine(MsgStr);
					Output.Close();
					Output = null;
				}
				if	( System.Diagnostics.Debugger.IsLogging() )
					System.Diagnostics.Debugger.Log( 0, "" , MsgStr + System.Environment.NewLine );
			}
                        catch (System.Exception Excpt) {
			}
		}

		public static void LogTo( string FileName )
		{	if (FileName == null)
				OutputFileName	=	null;
			else	OutputFileName	=	FileName.Trim();
		}

		public static void LogToConsole()
		{	LogTo("");
		}
	}
}