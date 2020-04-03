package com.hhstechgroup.dyp.fileupload;

import org.springframework.batch.item.ItemProcessor;

import com.hhstechgroup.dyp.fileupload.model.Report;

public class CustomProcessor implements ItemProcessor<Report, Report> {  
	   
	   @Override 
	   public Report process(Report item) throws Exception {  
	      System.out.println("Processing..." + item); 
	      return item; 
	   } 
	}