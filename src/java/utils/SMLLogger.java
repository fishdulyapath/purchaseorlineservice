package utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.LogLevel;

public class SMLLogger {
    private Class<?> clazz;
    private Logger logger;

    public SMLLogger(Class<?> clazz) {
        this.clazz = clazz;
        this.logger = LoggerFactory.getLogger(clazz);
    }
    
    public Logger getLogger(){
        return this.logger;
    }
    
    public void trace(String msg){
        this.logger.trace(msg);
    }
    
    public void debug(String msg){
        this.logger.debug(msg);
    }
    
    public void info(String msg){
        this.logger.info(msg);
    }
    
    public void warn(String msg){
        this.logger.warn(msg);
    }
    
    public void error(String msg){
        this.logger.error(msg);
    }
    
    public void log(String msg, LogLevel logLevel){
        switch(logLevel){
            case TRACE: this.trace(msg);
                break;
            case DEBUG: this.debug(msg);
                break;
            case INFO: this.info(msg);
                break;    
            case WARN: this.warn(msg);
                break;  
            case ERROR: this.error(msg);
                break;      
            default: this.trace(msg);
        }
    }
    
    public static void trace(Class<?> clazz, String msg){
        SMLLogger.initLogger(clazz).trace(msg);
    }
    
    public static void debug(Class<?> clazz, String msg){
        SMLLogger.initLogger(clazz).debug(msg);
    }
    
    public static void info(Class<?> clazz, String msg){
        SMLLogger.initLogger(clazz).info(msg);
    }
    
    public static void warn(Class<?> clazz, String msg){
        SMLLogger.initLogger(clazz).warn(msg);
    }
    
    public static void error(Class<?> clazz, String msg){
        SMLLogger.initLogger(clazz).error(msg);
    }
    
    public static void log(Class<?> clazz, String msg, LogLevel logLevel){
        switch(logLevel){
            case TRACE: SMLLogger.trace(clazz, msg);
                break;
            case DEBUG: SMLLogger.debug(clazz, msg);
                break;
            case INFO: SMLLogger.info(clazz, msg);
                break;    
            case WARN: SMLLogger.warn(clazz, msg);
                break;  
            case ERROR: SMLLogger.error(clazz, msg);
                break;      
            default: SMLLogger.trace(clazz, msg);
        }
    }
    
    private static SMLLogger initLogger(Class<?> clazz){
        return  new SMLLogger(clazz);
    }
}
