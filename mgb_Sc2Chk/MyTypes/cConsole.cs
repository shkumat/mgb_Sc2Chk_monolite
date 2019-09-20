// Версия 1.07  от 8 ноября 2016г.  Для рисования текстового окошка и менюшки

using	MyTypes;

namespace MyTypes
{
	public sealed class CConsole
	{
		public	static	readonly int	hWnd		=	GetConsoleWindow() ;

		public	const	byte	BLACK			=	0	;
		public	const	byte	DARKBLUE		=	1	;
		public	const	byte	DARKGREEN		=	2	;
		public	const	byte	DARKCYAN		=	3	;
		public	const	byte	DARKRED			=	4	;
		public	const	byte	DARKMAGENTA		=	5	;
		public	const	byte	DARKYELLOW		=	6	;
		public	const	byte	GRAY			=	7	;
		public	const	byte	DARKGRAY		=	8	;
		public	const	byte	BLUE			=	9	;
		public	const	byte	GREEN			=	10	;
		public	const	byte	CYAN			=	11	;
		public	const	byte	RED			=	12	;
		public	const	byte	MAGENTA			=	13	;
		public	const	byte	YELLOW			=	14	;
		public	const	byte	WHITE			=	15	;
		public	const	byte	DIALOG_BOX_OK		=	0	;
		public	const	byte	DIALOG_BOX_OK_		=	1	;
		public	const	byte	DIALOG_BOX_YES_NO	=	2	;
		public	const	byte	DIALOG_BOX_YES_NO_	=	3	;
		public	const	byte	DIALOG_BOX_STAY_CANCEL	=	4	;
		public	const	byte	DIALOG_BOX_STAY_CANCEL_	=	5	;
		public	const	byte	DIALOG_BOX_RETRY_CANCEL	=	6	;
		public	const	byte	DIALOG_BOX_RETRY_CANCEL_=	7	;

		public	const	System.ConsoleModifiers	KEY_ALT		=	System.ConsoleModifiers.Alt	;
		public	const	System.ConsoleModifiers	KEY_CTRL	=	System.ConsoleModifiers.Control	;
		public	const	System.ConsoleModifiers	KEY_SHIFT	=	System.ConsoleModifiers.Shift	;
		public	const	System.ConsoleKey	KEY_ESC		=	System.ConsoleKey.Escape	;
		public	const	System.ConsoleKey	KEY_TAB		=	System.ConsoleKey.Tab		;
		public	const	System.ConsoleKey	KEY_SPACE	=	System.ConsoleKey.Spacebar	;
		public	const	System.ConsoleKey	KEY_ENTER	=	System.ConsoleKey.Enter		;
		public	const	System.ConsoleKey	KEY_BACKSPACE	=	System.ConsoleKey.Backspace	;
		public	const	System.ConsoleKey	KEY_INSERT	=	System.ConsoleKey.Insert	;
		public	const	System.ConsoleKey	KEY_DELETE	=	System.ConsoleKey.Delete	;
		public	const	System.ConsoleKey	KEY_HOME	=	System.ConsoleKey.Home		;
		public	const	System.ConsoleKey	KEY_END		=	System.ConsoleKey.End		;
		public	const	System.ConsoleKey	KEY_PAGEUP	=	System.ConsoleKey.PageUp	;
		public	const	System.ConsoleKey	KEY_PAGEDOWN	=	System.ConsoleKey.PageDown	;
		public	const	System.ConsoleKey	KEY_UP		=	System.ConsoleKey.UpArrow	;
		public	const	System.ConsoleKey	KEY_DOWN	=	System.ConsoleKey.DownArrow	;
		public	const	System.ConsoleKey	KEY_LEFT	=	System.ConsoleKey.LeftArrow	;
		public	const	System.ConsoleKey	KEY_RIGHT	=	System.ConsoleKey.RightArrow	;
		public	const	System.ConsoleKey	KEY_COMMA	=	System.ConsoleKey.OemComma	;
		public	const	System.ConsoleKey	KEY_MINUS	=	System.ConsoleKey.OemMinus	;
		public	const	System.ConsoleKey	KEY_PLUS	=	System.ConsoleKey.OemPlus	;
		public	const	System.ConsoleKey	KEY_PAUSE	=	System.ConsoleKey.Pause		;
		public	const	System.ConsoleKey	KEY_PRINTSCREEN	=	System.ConsoleKey.PrintScreen;
		public	const	System.ConsoleKey	KEY_F1		=	System.ConsoleKey.F2;
		public	const	System.ConsoleKey	KEY_F2		=	System.ConsoleKey.F2;
		public	const	System.ConsoleKey	KEY_F3		=	System.ConsoleKey.F3;
		public	const	System.ConsoleKey	KEY_F4		=	System.ConsoleKey.F4;
		public	const	System.ConsoleKey	KEY_F5		=	System.ConsoleKey.F5;
		public	const	System.ConsoleKey	KEY_F6		=	System.ConsoleKey.F6;
		public	const	System.ConsoleKey	KEY_F7		=	System.ConsoleKey.F7;
		public	const	System.ConsoleKey	KEY_F8		=	System.ConsoleKey.F8;
		public	const	System.ConsoleKey	KEY_F9		=	System.ConsoleKey.F9;
		public	const	System.ConsoleKey	KEY_F10		=	System.ConsoleKey.F10;
		public	const	System.ConsoleKey	KEY_F11		=	System.ConsoleKey.F11;
		public	const	System.ConsoleKey	KEY_F12		=	System.ConsoleKey.F12;
		public	const	System.ConsoleKey	KEY_F13		=	System.ConsoleKey.F13;
		public	const	System.ConsoleKey	KEY_F14		=	System.ConsoleKey.F14;
		public	const	System.ConsoleKey	KEY_F15		=	System.ConsoleKey.F15;
		public	const	System.ConsoleKey	KEY_F16		=	System.ConsoleKey.F16;
		public	const	System.ConsoleKey	KEY_F17		=	System.ConsoleKey.F17;
		public	const	System.ConsoleKey	KEY_F18		=	System.ConsoleKey.F18;
		public	const	System.ConsoleKey	KEY_F19		=	System.ConsoleKey.F19;
		public	const	System.ConsoleKey	KEY_F20		=	System.ConsoleKey.F20;
		public	const	System.ConsoleKey	KEY_F21		=	System.ConsoleKey.F21;
		public	const	System.ConsoleKey	KEY_F22		=	System.ConsoleKey.F22;
		public	const	System.ConsoleKey	KEY_F23		=	System.ConsoleKey.F23;
		public	const	System.ConsoleKey	KEY_F24		=	System.ConsoleKey.F24;
		public	const	System.ConsoleKey	KEY_NumPad0	=	System.ConsoleKey.NumPad0;
		public	const	System.ConsoleKey	KEY_NumPad1	=	System.ConsoleKey.NumPad1;
		public	const	System.ConsoleKey	KEY_NumPad2	=	System.ConsoleKey.NumPad2;
		public	const	System.ConsoleKey	KEY_NumPad3	=	System.ConsoleKey.NumPad3;
		public	const	System.ConsoleKey	KEY_NumPad4	=	System.ConsoleKey.NumPad4;
		public	const	System.ConsoleKey	KEY_NumPad5	=	System.ConsoleKey.NumPad5;
		public	const	System.ConsoleKey	KEY_NumPad6	=	System.ConsoleKey.NumPad6;
		public	const	System.ConsoleKey	KEY_NumPad7	=	System.ConsoleKey.NumPad7;
		public	const	System.ConsoleKey	KEY_NumPad8	=	System.ConsoleKey.NumPad8;
		public	const	System.ConsoleKey	KEY_NumPad9	=	System.ConsoleKey.NumPad9;
		public	const	System.ConsoleKey	KEY_0		=	System.ConsoleKey.NumPad0;
		public	const	System.ConsoleKey	KEY_1		=	System.ConsoleKey.NumPad1;
		public	const	System.ConsoleKey	KEY_2		=	System.ConsoleKey.NumPad2;
		public	const	System.ConsoleKey	KEY_3		=	System.ConsoleKey.NumPad3;
		public	const	System.ConsoleKey	KEY_4		=	System.ConsoleKey.NumPad4;
		public	const	System.ConsoleKey	KEY_5		=	System.ConsoleKey.NumPad5;
		public	const	System.ConsoleKey	KEY_6		=	System.ConsoleKey.NumPad6;
		public	const	System.ConsoleKey	KEY_7		=	System.ConsoleKey.NumPad7;
		public	const	System.ConsoleKey	KEY_8		=	System.ConsoleKey.NumPad8;
		public	const	System.ConsoleKey	KEY_9		=	System.ConsoleKey.NumPad9;
		public	const	System.ConsoleKey	KEY_A		=	System.ConsoleKey.A ;
		public	const	System.ConsoleKey	KEY_B		=	System.ConsoleKey.B ;
		public	const	System.ConsoleKey	KEY_C		=	System.ConsoleKey.C ;
		public	const	System.ConsoleKey	KEY_D		=	System.ConsoleKey.D ;
		public	const	System.ConsoleKey	KEY_E		=	System.ConsoleKey.E ;
		public	const	System.ConsoleKey	KEY_F		=	System.ConsoleKey.F ;
		public	const	System.ConsoleKey	KEY_G		=	System.ConsoleKey.G ;
		public	const	System.ConsoleKey	KEY_H		=	System.ConsoleKey.H ;
		public	const	System.ConsoleKey	KEY_I		=	System.ConsoleKey.I ;
		public	const	System.ConsoleKey	KEY_J		=	System.ConsoleKey.J ;
		public	const	System.ConsoleKey	KEY_K		=	System.ConsoleKey.K ;
		public	const	System.ConsoleKey	KEY_L		=	System.ConsoleKey.L ;
		public	const	System.ConsoleKey	KEY_M		=	System.ConsoleKey.M ;
		public	const	System.ConsoleKey	KEY_N		=	System.ConsoleKey.N ;
		public	const	System.ConsoleKey	KEY_O		=	System.ConsoleKey.O ;
		public	const	System.ConsoleKey	KEY_P		=	System.ConsoleKey.P ;
		public	const	System.ConsoleKey	KEY_Q		=	System.ConsoleKey.Q ;
		public	const	System.ConsoleKey	KEY_R		=	System.ConsoleKey.R ;
		public	const	System.ConsoleKey	KEY_S		=	System.ConsoleKey.S ;
		public	const	System.ConsoleKey	KEY_T		=	System.ConsoleKey.T ;
		public	const	System.ConsoleKey	KEY_U		=	System.ConsoleKey.U ;
		public	const	System.ConsoleKey	KEY_V		=	System.ConsoleKey.V ;
		public	const	System.ConsoleKey	KEY_W		=	System.ConsoleKey.W ;
		public	const	System.ConsoleKey	KEY_X		=	System.ConsoleKey.X ;
		public	const	System.ConsoleKey	KEY_Y		=	System.ConsoleKey.Y ;
		public	const	System.ConsoleKey	KEY_Z		=	System.ConsoleKey.Z	;

		static readonly short[]	BORDER_SYMBOLS	=	{ 32, 32, 32, 32, 32, 32, 32, 32, 9472, 9472, 9474, 9474, 9484, 9488, 9492, 9496, 9552, 9552, 9553, 9553, 9556, 9559, 9562, 9565, 45, 45, 124, 124, 43, 43, 43, 43 };
		static readonly string[] DEFAULT_SHOW_BOX=	{ "", "   Подождите...   ", "" };

		private struct COORD
		{
    			public short X;
	    		public short Y;

    			public COORD(short X, short Y)
    			{
	        		this.X = X;
        			this.Y = Y;
    			}
		};

		[System.Runtime.InteropServices.DllImport("kernel32.dll", SetLastError = true)]
		private static extern int GetStdHandle(int nStdHandle);

		[System.Runtime.InteropServices.DllImport("kernel32.dll", ExactSpelling = true)]
		private static extern int GetConsoleWindow();

		[System.Runtime.InteropServices.DllImport("kernel32.dll")]
		private static extern bool ReadConsoleOutputCharacter(int hConsoleOutput, [System.Runtime.InteropServices.Out] byte[] lpCharacter, uint nLength, COORD dwReadCoord, out uint lpNumberOfCharsRead);

		[System.Runtime.InteropServices.DllImport("kernel32.dll")]
		private static extern bool ReadConsoleOutputAttribute(int hConsoleOutput, [System.Runtime.InteropServices.Out] byte[] lpCharacter, uint nLength, COORD dwReadCoord, out uint lpNumberOfCharsRead);

		[System.Runtime.InteropServices.DllImport("kernel32.dll")]
		private static extern bool WriteConsoleOutputCharacter(int hConsoleOutput, byte[] lpCharacter, uint nLength, COORD dwReadCoord, out uint lpNumberOfCharsWrite);

		[System.Runtime.InteropServices.DllImport("kernel32.dll")]
		private static extern bool WriteConsoleOutputAttribute(int hConsoleOutput, byte[] lpCharacter, uint nLength, COORD dwReadCoord, out uint lpNumberOfCharsWrite);

		static	System.ConsoleKeyInfo		Last_Key_Pressed	;
		static	int	OutHandle		=	GetStdHandle(-11); // -11 is the standard output stream. Odd number to use...
		static	int	CurrentLine		=	777		;
		static	int	FirstLine		=	0		;
		static	byte[]	SavedScreenAtr					;
		static	byte[]	SavedScreenChr					;
		static	byte[]	SavedScreenStr					;
		static	int	SavedScreenPosX					;
		static	int	SavedScreenPosY					;
		static	string[] Lines						;
		static	int	Height			=	5		;
		static	int	Width			=	16		;
		static	int	Left			=	3		;
		static	int	Top			=	2		;

		public	static	byte	BoxColor	=	GRAY * 16	;
		public	static	byte	MenuColor	=	GRAY		;
		public	static	int	BorderKind	=	2		;


		public	static	void	Flash() {
			CCommon.FlashWindow( hWnd , 0 );
		}
		
		public	static	void	Hide() {
			CCommon.ShowWindow( hWnd , CAbc.SW_HIDE );
		}
		
		public	static	void	Minimize() {
			CCommon.ShowWindow( hWnd , CAbc.SW_SHOWMINIMIZED );
		}

		public	static	void	Show() {
			CCommon.ShowWindow( hWnd , CAbc.SW_SHOWNORMAL );
		}
		
		public	static	void	HideCursor() {
			System.Console.CursorVisible = false;
		}

		public	static	void	ShowCursor() {
			System.Console.CursorVisible = true;
		}

		public	static	void Set_CtrlC_Handler(System.ConsoleCancelEventHandler Handler)
		{
			System.Console.CancelKeyPress += Handler;
			//
			//	Usage:	Set_CtrlC_Handler( addressOf On_CtrlC_Default )
			//
		}

		public static System.ConsoleKeyInfo ReadKeyInfo()
		{
			Last_Key_Pressed = System.Console.ReadKey(true);
			return Last_Key_Pressed;
		}

		public static System.ConsoleKey ReadKey()
		{
			return ReadKeyInfo().Key;
		}

		public static char ReadChar()
		{
			return ReadKeyInfo().KeyChar;
		}

		public static bool IsAltPressed()
		{
			return ((Last_Key_Pressed.Modifiers & KEY_ALT) != 0);
		}

		public static bool IsCtrlPressed()
		{
			return ((Last_Key_Pressed.Modifiers & KEY_CTRL) != 0);
		}

		public static bool IsShiftPressed()
		{
			return ((Last_Key_Pressed.Modifiers & KEY_SHIFT) != 0);
		}

		public static bool IsAnyKeyPressed()
		{
			return System.Console.KeyAvailable;
		}

		public static bool IsEscPressed()
		{
			return !(Last_Key_Pressed.Key != KEY_ESC);
		}

		public static bool IsEnterPressed()
		{
			return !(Last_Key_Pressed.Key != KEY_ENTER);
		}

		public static void ClearKeyboard()
		{
			char TmpC;
			while (IsAnyKeyPressed()) {
				TmpC = ReadChar();
			}
		}

		public static bool WaitForEscOrEnter()
		{
			char TmpC;
			ClearKeyboard();
			while (true) {
				TmpC = ReadChar();
				if (IsEscPressed())
					return false;
				if (IsEnterPressed())
					return true;
			}
		}

		public static void Clear()
		{
			System.Console.Clear();
		}

		public	static bool BreakWithCtrlC {
			get {
				return	( ! System.Console.TreatControlCAsInput );
			}
			set {
				System.Console.TreatControlCAsInput	=	!value;
			}
		}

		public static byte Color {
			get
                        {
				byte	Result	= (byte) System.Console.BackgroundColor ;
				Result		=	(byte) ( Result << 4 );
                                Result		=	(byte) ( Result + (byte)System.Console.ForegroundColor ) ;
                        	return	Result	;
                        }
			set {
				System.Console.ForegroundColor = (System.ConsoleColor) ( value & 15 );
				System.Console.BackgroundColor = (System.ConsoleColor) ( (value & 240) >> 4 ) ;
			}
		}

		public static void SaveScreen() {
			COORD	Coord;
			uint	Issue;
			int	I , J
			,	W	=	System.Console.WindowWidth
			,	H	=	System.Console.WindowHeight;
			SavedScreenPosX	=	System.Console.CursorLeft;
			SavedScreenPosY	=	System.Console.CursorTop;
			if	( SavedScreenAtr == null )
				SavedScreenAtr	= new	byte[ H * W * 2 ];
			else
				if	( SavedScreenAtr.Length < ( H * W * 2 ) )
					SavedScreenAtr	= new	byte[ H * W * 2 ];
			if	( SavedScreenChr == null )
				SavedScreenChr	= new	byte[ H * W * 2 ];
			else
				if	( SavedScreenChr.Length < ( H * W * 2 ) )
					SavedScreenChr	= new	byte[ H * W * 2 ];
			if	( SavedScreenStr == null )
				SavedScreenStr	= new	byte[ W*2 ];
			else
				if	( SavedScreenStr.Length < (W*2)  )
					SavedScreenStr	= new	byte[ W*2 ];
			for	( I = 0 ; I < H ; I++ ) {
				Coord.X=0;
				Coord.Y=(short)I;
				ReadConsoleOutputCharacter( OutHandle , SavedScreenStr , (uint) W ,  Coord, out Issue);
				Issue=Issue-(uint) W;
				for	( J=0 ; J < W*2 ; J++ )
					if	( Issue == 0 )
						SavedScreenChr[ I * W * 2 + J ] = SavedScreenStr[ J ];
					else
						SavedScreenChr[ I * W * 2 + J ] = 32;
				ReadConsoleOutputAttribute( OutHandle , SavedScreenStr , (uint) W ,  Coord, out Issue);
				Issue=Issue-(uint) W;
				for	( J=0 ; J < W*2 ; J++ )
					if	( Issue == 0 )
						SavedScreenAtr[ I * W * 2 + J ] = SavedScreenStr[ J ];
					else
						SavedScreenAtr[ I * W * 2 + J ] = 7;
			}
		}

		public static void RestoreScreen() {
			COORD	Coord;
			uint	Issue;
			int	I , J
			,	W	=	System.Console.WindowWidth
			,	H	=	System.Console.WindowHeight;
			if	( SavedScreenAtr == null )
				return;
			else
				if	( SavedScreenAtr.Length != ( H * W * 2 ) )
					return;
			if	( SavedScreenStr == null )
				return;
			else
				if	( SavedScreenStr.Length != (W*2)  )
					return;
			for	( I = 0 ; I < H ; I++ ) {
				Coord.X=0;
				Coord.Y=(short)I;
				for	( J=0 ; J < W*2 ; J++ )
					SavedScreenStr[ J ] = SavedScreenAtr[ I * W * 2 + J ] ;
				WriteConsoleOutputAttribute( OutHandle, SavedScreenStr, (uint) W ,  Coord, out Issue );
				for	( J=0 ; J < W*2 ; J++ )
					SavedScreenStr[ J ] = SavedScreenChr[  I * W * 2 + J ] ;
				WriteConsoleOutputCharacter( OutHandle, SavedScreenStr, (uint) W ,  Coord, out Issue );
			}
			System.Console.CursorLeft	=	SavedScreenPosX	;
			System.Console.CursorTop	=	SavedScreenPosY	;
		}

		static void Init()
		{
			int MaxWindowHeight = System.Console.WindowHeight - 5;
			if (Lines.Length > MaxWindowHeight) {
				Height = MaxWindowHeight;
				Top = 2;
			} else {
				Height = Lines.Length;
				Top = ((MaxWindowHeight - Lines.Length) >> 1) + 2;
			}
			int MaxLength = 0;
			int I;
			for (I = 0; I <= Lines.Length - 1; I++)
                        {
				if (Lines[I].Length > MaxLength)
                                	{
					MaxLength = Lines[I].Length;
					}
			}
			int MaxWindowWidth = System.Console.WindowWidth - 8;
			if (MaxLength > MaxWindowWidth) {
				Width = MaxWindowWidth;
				Left = 3;
			} else {
				Width = MaxLength;
				Left = ((MaxWindowWidth - MaxLength) >> 1) + 3;
			}
		}

		static void DrawShadow()
		{
			string EMPTY_SPACE = CCommon.Space(Width + 6);
			string TWO_SPACES = "  ";
			int I;
			for (I = 0; I <= Height + 2; I++) {
				System.Console.CursorLeft = Left + Width + 3;
				System.Console.CursorTop = Top + I - 1;
				System.Console.Write(TWO_SPACES);
			}
			System.Console.CursorLeft = Left - 1;
			System.Console.CursorTop = Top + Height + 2;
			System.Console.Write(EMPTY_SPACE);
		}

		static void DrawBox()
		{
			int I;
			string ONE_SPACE = " ";
			BorderKind=BorderKind & 3;
			System.Console.CursorLeft = Left - 3;
			System.Console.CursorTop = Top - 2;
			System.Console.Write(CCommon.Space(Width + 6));

			System.Console.CursorLeft = Left - 3;
			System.Console.CursorTop = Top - 1;
			System.Console.Write(ONE_SPACE);
			System.Console.Write(CCommon.Chr(BORDER_SYMBOLS[BorderKind * 8 + 4]));
			System.Console.Write(CCommon.Replicate(  CCommon.Chr(BORDER_SYMBOLS[BorderKind * 8]).ToString() , (Width + 2)));
			System.Console.Write(CCommon.Chr(BORDER_SYMBOLS[BorderKind * 8 + 5]));
			System.Console.Write(ONE_SPACE);

			string EMPTY_SPACE = CCommon.Space(Width + 2);
			for (I = 0; I <= Height - 1; I++) {
				System.Console.CursorLeft = Left - 3;
				System.Console.CursorTop = Top + I;
				System.Console.Write(ONE_SPACE);
				System.Console.Write(CCommon.Chr(BORDER_SYMBOLS[BorderKind * 8 + 2]));
				System.Console.Write(EMPTY_SPACE);
				System.Console.Write(CCommon.Chr(BORDER_SYMBOLS[BorderKind * 8 + 2]) + " ");
			}

			System.Console.CursorLeft = Left - 3;
			System.Console.CursorTop = Top + Height;
			System.Console.Write(ONE_SPACE);
			System.Console.Write(CCommon.Chr(BORDER_SYMBOLS[BorderKind * 8 + 6]));
			System.Console.Write(CCommon.Replicate(CCommon.Chr(BORDER_SYMBOLS[BorderKind * 8]).ToString(), Width + 2));
			System.Console.Write(CCommon.Chr(BORDER_SYMBOLS[BorderKind * 8 + 7]));
			System.Console.Write(ONE_SPACE);

			System.Console.CursorLeft = Left - 3;
			System.Console.CursorTop = Top + Height + 1;
			System.Console.Write(CCommon.Space(Width + 6));
		}

		static void DrawLines()
		{
			int I = FirstLine;
			while (((I < Lines.Length)) & ((I - FirstLine) < Height)) {
				System.Console.CursorLeft = Left;
				System.Console.CursorTop = Top + I - FirstLine;
				if ((I == CurrentLine))
					Color = MenuColor;
				if (Lines[I].Length < Width) {
					System.Console.Write(Lines[I]);
				} else {
					System.Console.Write(Lines[I].Substring(0, Width));
				}
				if ((I == CurrentLine))
					Color = BoxColor;
				I = I + 1;
			}
		}

		public static void ShowBox(params string[] StringList)
		{
			CurrentLine	=	99;
			FirstLine	=	0;
			if (StringList == null) {
				Lines = DEFAULT_SHOW_BOX;
			} else {
				if (StringList.Length == 0) {
					Lines = DEFAULT_SHOW_BOX;
				} else {
					Lines = StringList;
				}
			}
			int	I	=	StringList.Length-1	;
			while(I>=0)
				if( Lines[I--]==null )
					Lines[I+1]=" ";
			byte SavedColor = Color;
			Init();
			Color = DARKGRAY * 16;
			DrawShadow();
			Color = BoxColor;
			DrawBox();
			DrawLines();
			HideCursor();
			Color = SavedColor;
		}

		public static bool GetBoxChoice(params string[] StringList)
		{
			bool SavedCursor = System.Console.CursorVisible;
			ShowBox(StringList);
			System.Console.CursorVisible = false;
			bool Result = WaitForEscOrEnter();
			Clear();
			System.Console.CursorVisible = SavedCursor;
			return Result;
		}

		public static int GetMenuChoice(params string[] StringList)
		{
			if (StringList == null)
				return 0;
			if (StringList.Length == 0)
				return 0;
			CurrentLine = 0;
			bool SavedCursor = System.Console.CursorVisible;
			byte SavedColor = Color;
			System.ConsoleKeyInfo KeyInfo;
			ShowBox(StringList);
			CurrentLine	=	0;
			FirstLine	=	0;
			System.Console.CursorVisible = false;
			int	I	=	Lines.Length;
			while(I>0)
			{	I=I-1;
				Lines[I]=Lines[I]+CCommon.Replicate(" ",Width-Lines[I].Length);
			}
			do {
				DrawLines();
				KeyInfo = ReadKeyInfo();
				if ((KeyInfo.Key == KEY_DOWN) & (CurrentLine < (Lines.Length - 1))) {
					CurrentLine = CurrentLine + 1;
					if ((CurrentLine - FirstLine) > (Height - 1)) {
						FirstLine = FirstLine + 1;
					}
				}
				if ((KeyInfo.Key == KEY_UP) & (CurrentLine > 0)) {
					CurrentLine = CurrentLine - 1;
					if ((CurrentLine < FirstLine)) {
						FirstLine = FirstLine - 1;
					}
				}
			} while (!((IsEscPressed() | IsEnterPressed())));
			Color = SavedColor;
			Clear();
			System.Console.CursorVisible = SavedCursor;
			if (IsEnterPressed()) {
				return (CurrentLine + 1);
			} else {
				return 0;
			}
		}

		public static int DialogBox(byte Flag, params string[] SrcStringList) {
			if	( SrcStringList == null )
				return	0;
			string	TmpS		=	""	;
			int	MaxLength	=	28
			,	I		=	0
			,	KeysCount	=	0
			,	Result		=	1	;
			System.ConsoleKey	KeyCode		;
			string[] KeysStr	= new	string[ 3 ];
			string[] StringList	= new	string[ SrcStringList.Length + 3 ];
			for	( I=0 ; I<SrcStringList.Length ; I++ ) {
				TmpS	=	SrcStringList[ I ];
				if	( TmpS.Length > MaxLength )
					MaxLength	=	TmpS.Length;
				StringList[ I ]		=	TmpS;
			}
			I = SrcStringList.Length;
			StringList[ I ]	=	CCommon.RepStr( "_" , MaxLength);
			StringList[ I + 1 ]	=	" ";
			StringList[ I + 2 ]	=	" ";
			switch	( Flag & 15 ) {
				case	DIALOG_BOX_OK	: {
					KeysCount	=	1;
					KeysStr[0]	=	"[ Годится ]";
					break;
				}
				case	DIALOG_BOX_OK_	: {
					KeysCount	=	1;
					KeysStr[0]	=	"[ ОК ]";
					break;
				}
				case	DIALOG_BOX_YES_NO : {
					KeysCount	=	2;
					KeysStr[0]	=	"[ Да ]";
					KeysStr[1]	=	"[ Нет ]";
					break;
				}
				case	DIALOG_BOX_YES_NO_ : {
					KeysCount	=	2;
					KeysStr[0]	=	"[ Yes ]";
					KeysStr[1]	=	"[ No ]";
					break;
				}
				case	DIALOG_BOX_STAY_CANCEL : {
					KeysCount	=	2;
					KeysStr[0]	=	"[ Продолжить ]";
					KeysStr[1]	=	"[ Отмена ]";
					break;
				}
				case	DIALOG_BOX_STAY_CANCEL_	: {
					KeysCount	=	2;
					KeysStr[0]	=	"[ Continue ]";
					KeysStr[1]	=	"[ Cancel ]";
					break;
				}
				case	DIALOG_BOX_RETRY_CANCEL	: {
					KeysCount	=	2;
					KeysStr[0]	=	"[ Повторить ]";
					KeysStr[1]	=	"[ Отмена ]";
					break;
				}
				case	DIALOG_BOX_RETRY_CANCEL_ : {
					KeysCount	=	2;
					KeysStr[0]	=	"";
					KeysStr[1]	=	"";
					break;
				}
				default	: {
					KeysCount	=	0;
					break;
				}
			}
			if	( KeysCount==0 )
				return	0;
			for	( I=0 ; I<KeysCount ; I++ )
				MaxLength-=( KeysStr[I].Length + 1 );
			MaxLength = MaxLength >> 1 ;
			bool SavedCursor = System.Console.CursorVisible;
			ShowBox(StringList);
			System.Console.CursorVisible=false;
			byte SavedColor = Color;
			do {
				System.Console.CursorLeft = Left + MaxLength + 1 ;
				System.Console.CursorTop = Top + SrcStringList.Length + 2;
				for	( I=0 ; I<KeysCount ; I++ ) {
					if	( Result == (I+1) )
						Color=MenuColor;
					else
						Color=BoxColor;
					System.Console.Write(KeysStr[I]);
					Color=BoxColor;
					System.Console.Write(" ");
				}
				KeyCode		=	ReadKey();
				if	( KeysCount>1 ) {
					if	( ( KeyCode == CConsole.KEY_RIGHT ) || ( KeyCode == CConsole.KEY_DOWN ) || ( KeyCode == CConsole.KEY_6 ) || ( KeyCode == CConsole.KEY_2 ) )
						Result++;
					if	( ( KeyCode == CConsole.KEY_LEFT ) || ( KeyCode == CConsole.KEY_UP ) || ( KeyCode == CConsole.KEY_4 ) || ( KeyCode == CConsole.KEY_8 ) )
						Result--;
					if	(Result==0)
						Result=KeysCount;
					if	(Result>KeysCount)
						Result=1;
				}
			} while ( (KeyCode!=CConsole.KEY_ESC) && (KeyCode!=CConsole.KEY_ENTER) );
			if	(  IsEscPressed() )
				Result=0;
			System.Console.CursorVisible = false;
			Color=SavedColor;
			Clear();
			System.Console.CursorVisible = SavedCursor;
			return	Result;
		}
	}

}