from geo.Geoserver import Geoserver


class geoserver_fn:
   
    
    
    def __init__(self):
    
    
        try:
            self.geo=Geoserver('http://127.0.0.1:8080/geoserver/',username='admin', password='geoserver')
            
        except Exception as er:
            print('Geoserver Error:',er)
    
    
    
    def create_workspace(self,wksp_name):
        # For creating workspace
        try:
            self.geo.create_workspace(workspace=wksp_name)
            return 'Workspace successfully created in Geoserver:', wksp_name
            
        except Exception as er1:
            return 'Geoserver workspace not created Error:',er1
        
        
        
        
    def del_workspace(self,wspace_name):
        # For deleting workspace 
        try:
            self.geo.delete_workspace(workspace=wspace_name)
            return 'Workspace successfully deleted from Geoserver:',  wspace_name
           
        except Exception as er2:
            return 'Geoserver workspace not deleted Error:',er2  
            
        
        
        
        
    def create_datastore(self,dstore_name,wksp,dbname,hst,pguser,pwd):
        #create_datastore(self,dstore_name,store_name,workspace,db,host,pg_user,pg_password):
        # For creating postGIS connection/datastore 
        #Please always use existing workspace name
        try:
            
            self.geo.create_featurestore(store_name=dstore_name, 
                                            workspace=wksp, 
                                            db=dbname,
                                            host=hst, 
                                            pg_user=pguser,
                                            pg_password=pwd
                                            )
            
            return 'Datstore successfully created in Geoserver:', dstore_name
            
        except Exception as er3:
            return 'Geoserver datastore not created Error:', er3
    
    
    
    def create_raster_layer(self,titl_name,pfad,wkp):
        # For publishing/ uploading raster data from local to the geoserver

        try:
            
            geo.create_coveragestore(layer_name=titl_name, 
                                     path=pfad, 
                                     workspace=wkp
                                     )
            
            return 'Raster Layer from local created in Geoserver:', titl_name
        
        except Exception as er4:
            return 'Geoserver create shape layer Error:',er4
    
        
        
    def create_shp_layer(self,wks,strnam,pgtab,tit):
        #For publishing layer from db to geoserver
        #Default styles are allocated to uploaded layers from Geoserver automatically
        try:
            self.geo.publish_featurestore(workspace=wks, 
                                         store_name=strnam, 
                                         pg_table=pgtab, 
                                         title=tit
                                         )
            
            return 'Shape Layer created in Geoserver:', tit 
        except Exception as er5:
            return 'Geoserver create shape layer Error:',er5
    
    
    
    def create_shp_style(self,shp_pfad,wks):
        # For uploading SLD file from local computer into geoserver
        #Stylename will automatically be taken from the .sld file name
        try:
            
            self.geo.upload_style(path=shp_pfad, workspace=wks)
            return 'Shapefile-layer Style successfully created in Geoserver' 
        except Exception as er6:
            return 'Shapefile-layer style create Error:',er6
        
        
    def allocate_style_to_shp(self,shp_name,sty_name,wks):
        
        try:
            self.geo.publish_style(layer_name=shp_name, style_name=sty_name, workspace=wks)
        except Exception as er7:
            return 'Error' + ' ' + sty_name + ' ' +'not allocated to' + ' ' +shp_name + ' '+ 'in geoserver'
            return er7

    
    
    
    #---------Define Raster style
    def create_raster_style(self,local_pfad,sty_name,wkp,color_hue='RdYiGn'):
        # For creating the style file for raster data dynamically and connect it with layer
        
        try:
            self.geo.create_coveragestyle(raster_path=local_pfad, 
                                          style_name=sty_name, 
                                          workspace=wkp,
                                          color_ramp=color_hue)
            return 'Raster Style successfully created in Geoserver:', sty_name
        except Exception as er8:
            return 'Raster style create Error:', er8
        
        
    def allocate_style_to_raster(self,rast_name,st_nam,wks):
        #For giving already published raster layer style conventions with already existing styles 
        
        try:
            self.geo.publish_style(layer_name=rast_name, style_name=st_nam, workspace=wks)
            return st_nam + ' ' +'allocated to' + ' ' + rast_name + ' '+ 'in geoserver'  
        except Exception as er9:
            return 'Error'+ ' ' + st_nam + ' ' +'not allocated to' + ' ' + rast_name + ' '+ 'in geoserver'  
            return er9
            
            
    def del_layer(self,ly_call,wks):
        #For deleting layer
        try:
            self.geo.delete_layer(layer_name=ly_call, workspace=wks)
            return 'Layer deleted from Geoserver:',  ly_call
           
        except Exception as er10:
            return 'Geoserver delete layer Error:',er10
        
        
        
        
    def del_style(self,sty_name,wks):
        #For deleting style
        try:
            self.geo.delete_style(style_name=sty_name, workspace=wks)
            return 'Style successfully deleted from Geoserver:',  sty_name
           
        except Exception as er11:
            return 'Geoserver delete style Error:',er11
            


