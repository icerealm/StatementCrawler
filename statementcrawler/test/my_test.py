

import re
    
input = "w"
pattern = "^(" + input.capitalize() + "|" +input.lower() + ").*"

text = "WORLD"

found = re.match(pattern,text)
if found:
    print("True")
        