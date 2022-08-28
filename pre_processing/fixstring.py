from ftfy import fix_text
import re

chars_to_remove = [")","(","|","[","]","{","}", ","]
badcharsrx = re.compile('[' + re.escape(''.join(chars_to_remove)) + ']') # Replace quotes and commas.

def fix_string(string):
    string = str(string)
    string = fix_text(string) # fix text
    string = string.encode("ascii", errors="ignore").decode() #remove non ascii chars
    string = string.lower()
    string = badcharsrx.sub('', string)
    string = string.replace('&', 'and')
    string = string.replace(',', ' ')
    string = string.replace("'", 'ft')
    string = string.replace('\"', 'in')    
    string = string.lower() # normalise case
    string = re.sub(' +',' ',string).strip() # get rid of multiple spaces and replace with a single
    return string