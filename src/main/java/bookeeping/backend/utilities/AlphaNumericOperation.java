package bookeeping.backend.utilities;

import java.util.Arrays;

public class AlphaNumericOperation
{
	private static String[] baseCharacters = {
		"0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
		"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
		"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"
	};
	
	public static String convertFromBase10(int base10Number)
	{
		String conversionValue = "";
		int baseCharactersSize = AlphaNumericOperation.baseCharacters.length;
		
		int multiplier = base10Number;
		do
		{
			int addition = multiplier % baseCharactersSize;
			conversionValue = AlphaNumericOperation.baseCharacters[addition] + conversionValue;
			multiplier = multiplier / baseCharactersSize;
		}while(multiplier > 0);
		
		return conversionValue;
	}
	
	public static String add(String number, int incrementBy)
	{
		String incrementedNumber = "";
		String incrementValue = AlphaNumericOperation.convertFromBase10(incrementBy);
		int baseCharactersSize = AlphaNumericOperation.baseCharacters.length;
		
		int difference = number.length() - incrementValue.length();
		String zeroString = "";
		for(int i = 0; i < Math.abs(difference); i++)
		{
			zeroString += "0";
		}
		
		if(difference > 0)
		{
			incrementValue = zeroString + incrementValue;
		}
		else if(difference < 0)
		{
			number = zeroString + number;
		}
		
		int loopLength = number.length();
		int carry = 0;
		while(loopLength > 0)
		{
			int baseCharacterIndexForNumber = Arrays.asList(AlphaNumericOperation.baseCharacters).indexOf(number.substring(number.length() - 1));
			int baseCharacterIndexForIncrementValue = Arrays.asList(AlphaNumericOperation.baseCharacters).indexOf(incrementValue.substring(incrementValue.length() - 1));
			
			int newBaseCharacterIndex = baseCharacterIndexForNumber + baseCharacterIndexForIncrementValue + carry;
			if(newBaseCharacterIndex >= baseCharactersSize)
			{
				carry = newBaseCharacterIndex / baseCharactersSize;
				newBaseCharacterIndex = newBaseCharacterIndex % baseCharactersSize;
				incrementedNumber = AlphaNumericOperation.baseCharacters[newBaseCharacterIndex] + incrementedNumber;
			}
			else
			{
				carry = 0;
				incrementedNumber = AlphaNumericOperation.baseCharacters[newBaseCharacterIndex] + incrementedNumber;
			}
			
			number = number.substring(0, number.length() - 1);
			incrementValue = incrementValue.substring(0, incrementValue.length() - 1);
			loopLength--;
		}
		
		if(carry > 0)
		{
			int multiplier = carry;
			do
			{
				int addition = multiplier % baseCharactersSize;
				incrementedNumber = AlphaNumericOperation.baseCharacters[addition] + incrementedNumber;
				multiplier = multiplier / baseCharactersSize;
			}while(multiplier > 0);
		}
		
		return incrementedNumber;
	}
}
