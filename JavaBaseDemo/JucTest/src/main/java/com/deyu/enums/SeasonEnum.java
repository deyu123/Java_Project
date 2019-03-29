package com.deyu.enums;

import java.util.Iterator;

public enum SeasonEnum
{
	ONE(1,"spring"),TWO(2,"summer"),THREE(3,"autumn"),FOUR(4,"winner");
	
	private Integer retCode;
	private String  retMessage;
	
	/**
	 * @param retCode
	 * @param retMessage
	 */
	private SeasonEnum(Integer retCode, String retMessage)
	{
		this.retCode = retCode;
		this.retMessage = retMessage;
	}

	public Integer getRetCode()
	{
		return retCode;
	}

	public void setRetCode(Integer retCode)
	{
		this.retCode = retCode;
	}

	public String getRetMessage()
	{
		return retMessage;
	}

	public void setRetMessage(String retMessage)
	{
		this.retMessage = retMessage;
	}
	
	public static SeasonEnum foreachSeasonEnum(Integer index)
	{
		for (SeasonEnum element : values()) 
		{
			if(element.getRetCode() == index)
			{
				return element;
			}
		}
		return null;
	}
	
	
}
