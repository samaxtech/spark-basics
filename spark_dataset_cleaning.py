import re 

## 2. Extract Line Numbers ##
raw_hamlet = sc.textFile("data/hamlet.txt")
split_hamlet = raw_hamlet.map(lambda line: line.split('\t'))

def format_id(line):
    line_id = [s for s in re.findall(r'-?\d+\.?\d*', line[0])]
    
    new_line = list()
    new_line.append(line_id[0])
    
    if len(line) > 1:
        for i in line[1:]:
            new_line.append(i)
    
    return new_line
        
        
hamlet_with_ids = split_hamlet.map(lambda line: format_id(line))
hamlet_with_ids.take(10)

## 3. Remove Blank Values ##

def notEmpty(line):
    if len(line) == 1:
        return False
    return True

not_empty_hamlet = hamlet_with_ids.filter(notEmpty)
hamlet_text_only = not_empty_hamlet.map(lambda line: [x for x in line if x != ''])
hamlet_text_only.take(10)

## 4. Remove Pipe Characters ##

def replace_pipe(line):
    new_line = list()

    for x in line:
        if x == '|':
            pass
        elif '|' in x:
            new_line.append(x.replace("|", ''))
        else: 
            new_line.append(x)

    return new_line

clean_hamlet = hamlet_text_only.map(lambda line: replace_pipe(line))
clean_hamlet.take(10)