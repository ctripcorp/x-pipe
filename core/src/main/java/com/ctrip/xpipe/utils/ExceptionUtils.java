package com.ctrip.xpipe.utils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public class ExceptionUtils {

    protected static String[] CAUSE_METHOD_NAMES = {
            "getCause",
            "getNextException",
            "getTargetException",
            "getException",
            "getSourceException",
            "getRootCause",
            "getCausedByException",
            "getNested"
        };

    public static String getStackTrace( Throwable t )
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter( sw, true );
        t.printStackTrace( pw );
        return sw.getBuffer().toString();
    }

    public static String getFullStackTrace( Throwable t )
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter( sw, true );
        Throwable[] ts = getThrowables( t );
        for ( Throwable t1 : ts )
        {
            t1.printStackTrace( pw );
            if ( isNestedThrowable( t1 ) )
            {
                break;
            }
        }
        return sw.getBuffer().toString();
    }

    public static Throwable[] getThrowables( Throwable throwable )
    {
        List<Throwable> list = new ArrayList<Throwable>();
        while ( throwable != null )
        {
            list.add( throwable );
            throwable = getCause( throwable );
        }
        return list.toArray( new Throwable[list.size()] );
    }


    public static Throwable getCause( Throwable throwable )
    {
        return getCause( throwable, CAUSE_METHOD_NAMES );
    }

    public static Throwable getCause( Throwable throwable, String[] methodNames )
    {
        Throwable cause = getCauseUsingWellKnownTypes( throwable );
        if ( cause == null )
        {
            for ( String methodName : methodNames )
            {
                cause = getCauseUsingMethodName( throwable, methodName );
                if ( cause != null )
                {
                    break;
                }
            }

            if ( cause == null )
            {
                cause = getCauseUsingFieldName( throwable, "detail" );
            }
        }
        return cause;
    }

    private static Throwable getCauseUsingWellKnownTypes( Throwable throwable )
    {
        if ( throwable instanceof SQLException )
        {
            return ( (SQLException) throwable ).getNextException();
        }
        else if ( throwable instanceof InvocationTargetException )
        {
            return ( (InvocationTargetException) throwable ).getTargetException();
        }
        else
        {
            return null;
        }
    }

    private static Throwable getCauseUsingMethodName( Throwable throwable, String methodName )
    {
        Method method = null;
        try
        {
            method = throwable.getClass().getMethod( methodName, null );
        }
        catch ( NoSuchMethodException ignored )
        {
        }
        catch ( SecurityException ignored )
        {
        }

        if ( method != null && Throwable.class.isAssignableFrom( method.getReturnType() ) )
        {
            try
            {
                return (Throwable) method.invoke( throwable, new Object[0] );
            }
            catch ( IllegalAccessException ignored )
            {
            }
            catch ( IllegalArgumentException ignored )
            {
            }
            catch ( InvocationTargetException ignored )
            {
            }
        }
        return null;
    }

    private static Throwable getCauseUsingFieldName( Throwable throwable, String fieldName )
    {
        Field field = null;
        try
        {
            field = throwable.getClass().getField( fieldName );
        }
        catch ( NoSuchFieldException ignored )
        {
        }
        catch ( SecurityException ignored )
        {
        }

        if ( field != null && Throwable.class.isAssignableFrom( field.getType() ) )
        {
            try
            {
                return (Throwable) field.get( throwable );
            }
            catch ( IllegalAccessException ignored )
            {
            }
            catch ( IllegalArgumentException ignored )
            {
            }
        }
        return null;
    }

    public static boolean isNestedThrowable( Throwable throwable )
    {
        if ( throwable == null )
        {
            return false;
        }

        if ( throwable instanceof SQLException )
        {
            return true;
        }
        else if ( throwable instanceof InvocationTargetException )
        {
            return true;
        }

        for ( String CAUSE_METHOD_NAME : CAUSE_METHOD_NAMES )
        {
            try
            {
                Method method = throwable.getClass().getMethod( CAUSE_METHOD_NAME, null );
                if ( method != null )
                {
                    return true;
                }
            }
            catch ( NoSuchMethodException ignored )
            {
            }
            catch ( SecurityException ignored )
            {
            }
        }

        try
        {
            Field field = throwable.getClass().getField( "detail" );
            if ( field != null )
            {
                return true;
            }
        }
        catch ( NoSuchFieldException ignored )
        {
        }
        catch ( SecurityException ignored )
        {
        }

        return false;
    }

}
