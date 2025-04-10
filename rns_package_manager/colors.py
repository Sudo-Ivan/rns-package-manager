"""
Provides simple ANSI color codes for terminal output.
"""

# ANSI Escape Codes
RESET = '\033[0m'
BOLD = '\033[1m'
DIM = '\033[2m' # Not always supported
UNDERLINE = '\033[4m'

# Foreground Colors
BLACK = '\033[30m'
RED = '\033[31m'
GREEN = '\033[32m'
YELLOW = '\033[33m'
BLUE = '\033[34m'
MAGENTA = '\033[35m'
CYAN = '\033[36m'
WHITE = '\033[37m'
BRIGHT_BLACK = '\033[90m' # Grey
BRIGHT_RED = '\033[91m'
BRIGHT_GREEN = '\033[92m'
BRIGHT_YELLOW = '\033[93m'
BRIGHT_BLUE = '\033[94m'
BRIGHT_MAGENTA = '\033[95m'
BRIGHT_CYAN = '\033[96m'
BRIGHT_WHITE = '\033[97m'

# Background Colors
BG_BLACK = '\033[40m'
BG_RED = '\033[41m'
BG_GREEN = '\033[42m'
BG_YELLOW = '\033[43m'
BG_BLUE = '\033[44m'
BG_MAGENTA = '\033[45m'
BG_CYAN = '\033[46m'
BG_WHITE = '\033[47m'
BG_BRIGHT_BLACK = '\033[100m' # Grey
BG_BRIGHT_RED = '\033[101m'
BG_BRIGHT_GREEN = '\033[102m'
BG_BRIGHT_YELLOW = '\033[103m'
BG_BRIGHT_BLUE = '\033[104m'
BG_BRIGHT_MAGENTA = '\033[105m'
BG_BRIGHT_CYAN = '\033[106m'
BG_BRIGHT_WHITE = '\033[107m'

def colorize(text, *codes):
    """Applies ANSI codes to the text and ensures it's reset afterward."""
    return f"{''.join(codes)}{text}{RESET}"

# --- Helper Functions ---

def error(text):
    """Formats text as a bold red error message."""
    return colorize(text, BOLD, RED)

def warning(text):
    """Formats text as a yellow warning message."""
    return colorize(text, YELLOW)

def success(text):
    """Formats text as a green success message."""
    return colorize(text, GREEN)

def info(text):
    """Formats text as a cyan informational message."""
    return colorize(text, CYAN)

def detail(text):
    """Formats text with a dimmer color (grey)."""
    return colorize(text, BRIGHT_BLACK) # Using bright black as 'dim'

def prompt(text):
     """Formats text as a bold prompt."""
     return colorize(text, BOLD) 