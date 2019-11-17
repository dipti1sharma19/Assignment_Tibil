package testfortest;

import java.util.Scanner;
import java.util.regex.Pattern;

public class MyRegex {

	private static final Pattern PATTERN = Pattern.compile(
	        "^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$");
	
	public static boolean validate(final String ip) {
	    return PATTERN.matcher(ip).matches();
	}
	
	public static void main(String args[]){
		Scanner sc=new Scanner(System.in);
		while(sc.hasNext()){
			System.out.println(validate(sc.nextLine()));
		}
	}
}
