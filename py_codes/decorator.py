import functools

        
    
def error_handler(caller):
    
    print('Decorator called')
 
    #---functools.wraps lets the called function retain its original identity when called with functionname.__name__
    @functools.wraps(caller)
    def error_handler_wrapper(*args,**kwargs):
        print('calling function') 
        
        
        try:
            
            caller(*args,**kwargs)
        except Exception as er1:
            print('Error experienced was:',er1)
        
        
        
        print('Function call is over')
    
    return error_handler_wrapper
        
            
        
    print('first call')
        
    
    return error_handler_wrapper 
    
        
    
    

@error_handler
def add(*args,**kwargs):
    print('Executing function')

    for i in args:
        #if an argument is not a type of integer, the exception above would be triggered
        dat=i+ 10

        print (f'{i} + 10= {dat}')

    for x,y in kwargs.items():
        # if a value is not a type of integer, the exception above would be triggered
        mat= y + 10
        print(f'if {x} is added to 10, the result would be {mat}')

    

    print('.....................')
    #to return values, use the return command
    #return arguments


    print('.....................')


if __name__=='__main__':
        add(6,'c')
    #print(f'Function name is={add.__name__}')



