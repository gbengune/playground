from geo.Geoserver import Geoserver
import functools
#Importing geoserver credentials from utils
#.env variables are all combined in utils file
from utils import geos_creds

import logging 

 
#-----------------------------------------------------------------------------------------------------
#Defining a file logger
#File logging could also be triggered from an utils file 

gs_logger= logging.getLogger(__name__)

#Logging File would always be appended into specified source file in cwd of module
logging.basicConfig(
                    filename="geoserver_activities.log",
                    level=logging.DEBUG,
                    filemode="a",
                   
                    format="%(levelname)s - %(asctime)s - %(filename)s -  %(lineno)s - %(message)s"
                    )



#----------------------------------------------------------------------------------------------------
#Defining a decorator for non-class based function exceptions
#This decorator can be utilised instead of using try except triggers repeatedly in code
#Though it is better doing that without using a class

def my_decorator(f_caller):
   
    #code below helps retain method properties
    @functools.wraps(f_caller)
    
    
    #Robust way of accepting all possible arguments or parameter key value pairs
    def wrapper(*args,**kwargs):
        print( f'Something is happening before the function is called.')
        gs_logger.info({f'@ my_docorator-Something is happening before the function is called.'})
        
        
        try:
            f_caller(*args,**kwargs)
            
            
            
        except Exception as error1:
            raise f'Error instance:,{error1}'
            gs_logger.exception( {f'Error instance:,{error1}'})
               
        print(f'Something is happening after the function is called.')
        gs_logger.info({f'@ my_docorator-Something is happening after the function is called.'})
           
    return wrapper




#-----------------------------------------------------------------------------------------------------    
#Creating a global class element for all geoserver RespAPI methods   
class geoserver_fn:
   
    
    #Defining a global class initiator for geoserver connection credentials 
    def __init__(self):
    
        
        try:
            self.geo=Geoserver(geos_creds['host'],username=geos_creds['username'], password=geos_creds['password'])
            
            #print(f'Geoserver Connection Successfull')
            gs_logger.info({f'Geoserver Connection Successfull'})
        
            
        
        
        #General unit exception handlers used everywhere
        
        #For production environments, specific exception handlers need to be \n
        #explicitly defined in env file and then triggered uniquely
        
        #A decorator can be written and used as a single exception handler for all instances (see above).
        
        except Exception as er:
            
            
            raise f'Geoserver Error: {er}'
            gs_logger.exception( {f'Geoserver Error: {er}'})
    
    
    
    
    def create_workspace(self,wksp_name):
        # For creating workspace
        try:
            self.geo.create_workspace(workspace=wksp_name)
            #print( f'Workspace successfully created in Geoserver: {wksp_name}')
            gs_logger.info({f'Workspace successfully created in Geoserver: {wksp_name}'})
        
        
        except Exception as er1:
            raise f'Geoserver workspace not created Error: {er1}'
            gs_logger.exception( {f'Geoserver workspace not created Error: {er1}'})
        
        
        
        
    def del_workspace(self,wspace_name):
        # For deleting workspace 
        
        try:
            self.geo.delete_workspace(workspace=wspace_name)
            #print( f'Workspace successfully deleted from Geoserver: {wspace_name}')
            gs_logger.info({f'Workspace successfully deleted from Geoserver: {wspace_name}'})
            
        
        except Exception as er2:
            raise f'Geoserver workspace not deleted Error: {er2}'  
            gs_logger.exception( {f'Geoserver workspace not deleted Error: {er2}'})
        
        
        
        
    def create_datastore(self,dstore_name,wksp,dbname,hst,pguser,pwd):
        
        #Create_datastore(self,dstore_name,store_name,workspace,db,host,pg_user,pg_password):
        #For creating postGIS connection/datastore 
        #Please always use existing workspace name
        
        
        try:
            
            self.geo.create_featurestore(store_name=dstore_name, 
                                            workspace=wksp, 
                                            db=dbname,
                                            host=hst, 
                                            pg_user=pguser,
                                            pg_password=pwd
                                            )
            
            #print( f'Datastore successfully created in Geoserver: {dstore_name}')
            gs_logger.info({f'Datastore successfully created in Geoserver: {dstore_name}'})
            
            
        except Exception as er3:
            raise f'Geoserver datastore not created Error: {er3}'
            gs_logger.exception( {f'Geoserver datastore not created Error: {er3}'})
            
    
    
    def create_raster_layer(self,titl_name,pfad,wkp):
        # For publishing/ uploading raster data from local to the geoserver

        try:
            
            self.geo.create_coveragestore(layer_name=titl_name, 
                                     path=pfad, 
                                     workspace=wkp
                                     )
            
            #print( f'Raster Layer from local created in Geoserver: {titl_name}')
            gs_logger.info({f'Raster Layer from local created in Geoserver: {titl_name}'})
            
            
            
        except Exception as er4:
            raise f'Geoserver create raster shape layer Error: {er4}'
            gs_logger.exception( {f'Geoserver create raster shape layer Error: {er4}'})
        
        
        
    def create_shp_layer(self,wks,strnam,pgtab,tit):
        #For publishing layer from db to geoserver
        #Default styles are allocated to uploaded layers from Geoserver automatically
        
        try:
            self.geo.publish_featurestore(workspace=wks, 
                                         store_name=strnam, 
                                         pg_table=pgtab, 
                                         title=tit
                                         )
            
            #print( f'Shape Layer created in Geoserver: {tit}')
            gs_logger.info({f'Shape Layer created in Geoserver: {tit}'})
            
            
        except Exception as er5:
            raise f'Geoserver create shape layer Error: {er5}'
            gs_logger.exception( {f'Geoserver create shape layer Error: {er5}'})
    
    
    
    
    def create_shp_style(self,shp_pfad,wks):
        # For uploading SLD file from local computer into geoserver
        #Stylename will automatically be taken from the .sld file name
        try:
            
            self.geo.upload_style(path=shp_pfad, workspace=wks)
            
            #print( f'Shapefile-layer Style successfully created in Geoserver' )
            gs_logger.info({f'Shapefile-layer Style successfully created in Geoserver'})
            
            
        except Exception as er6:
            raise f'Shapefile-layer style create Error: {er6}'
            gs_logger.exception( {f'Shapefile-layer style create Error: {er6}'})
            
            
        
    def allocate_style_to_shp(self,shp_name,sty_name,wks):
        
        try:
            self.geo.publish_style(layer_name=shp_name, style_name=sty_name, workspace=wks)
            
            #print( f'Shapefile {shp_name} published successfully in Geoserver using style: {sty_name} ' )
            
            gs_logger.info({f'Shapefile {shp_name} published successfully in Geoserver using style: {sty_name} '})
            
            
            
        except Exception as er7:
            raise f'Error encountered publishing {shp_name} using {sty_name} in geoserver'
            raise er7
            
            gs_logger.exception( {f'Error encountered publishing {shp_name} using {sty_name} in geoserver'})
            

    
    
    
    #---------Define Raster style
    def create_raster_style(self,local_pfad,sty_name,wkp,color_hue='RdYiGn'):
        # For creating the style file for raster data dynamically and connect it with layer
        
        try:
            self.geo.create_coveragestyle(raster_path=local_pfad, 
                                          style_name=sty_name, 
                                          workspace=wkp,
                                          color_ramp=color_hue)
            #print( f'Raster Style {sty_name} successfully created in Geoserver')
            gs_logger.info({f'Raster Style {sty_name} successfully created in Geoserver'})
            
        except Exception as er8:
            raise f'Error creating Raster style {sty_name} in geoserver {er8}'
            
            gs_logger.exception( { f'Error creating Raster style {sty_name} in geoserver {er8}'})
        
        
        
    def allocate_style_to_raster(self,rast_name,st_nam,wks):
        #For giving already published raster layer style conventions with already existing styles 
        
        try:
            self.geo.publish_style(layer_name=rast_name, style_name=st_nam, workspace=wks)
            
            
            #print( f'{st_nam} allocated to {rast_name} in geoserver')
            gs_logger.info({f'{st_nam} allocated to {rast_name} in geoserver'})
            
            
        except Exception as er9:
            
            raise f'Error {st_nam} not allocated to {rast_name} in geoserver'  
            raise er9
            
            gs_logger.exception( {f'Error {st_nam} not allocated to {rast_name} in geoserver'})
            
            
            
    def del_layer(self,ly_call,wks):
        
        #For deleting layer
        
        try:
            self.geo.delete_layer(layer_name=ly_call, workspace=wks)
            #print( f'Layer {ly_call} deleted from Geoserver')
           
            gs_logger.info({f'Layer {ly_call} deleted from Geoserver'})
            
        except Exception as er10:
            raise f'Error deleting layer {ly_call} in Geoserver {er10}'
            
            gs_logger.exception( {f'Error deleting layer {ly_call} in Geoserver {er10}'})
        
        
        
    def del_style(self,sty_name,wks):
        #For deleting style
        try:
            self.geo.delete_style(style_name=sty_name, workspace=wks)
            #print( f'Style {sty_name} successfully deleted from Geoserver')
           
            gs_logger.info({f'Style {sty_name} successfully deleted from Geoserver'})
            
        except Exception as er11:
            raise f'Geoserver delete style Error: {er11}'
            
            gs_logger.exception( {f'Geoserver delete style Error: {er11}'})
            
            
            
#END            



#Methods Caller


if __name__=='__main__':
    geoserver_fn()
    #.methodhere()

    
    
    