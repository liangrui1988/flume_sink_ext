package com.flume.dome.file;

import java.util.Locale;

import org.apache.flume.formatter.output.PathManager;
import org.apache.flume.formatter.output.PathManagerType;;

public class Main {

	// private PathManager pathController=DatePathManager.Builder.build;

	public static void main(String[] args)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException {

		// Context pathManagerContext = new Context();
		// String managerType = "com.flume.dome.file.DatePathManager.Builder";
		String managerType = "ROLLTIME";
		PathManagerType type = null;
		// try to find builder class in enum of known output serializers
		try {
			type = PathManagerType.valueOf(managerType.toUpperCase(Locale.ENGLISH));
		} catch (IllegalArgumentException e) {
			// logger.debug("Not in enum, loading builder class: {}",
			// managerType);
			type = PathManagerType.OTHER;
		}
		Class<? extends PathManager.Builder> builderClass = type.getBuilderClass();

		System.out.println(builderClass);

		managerType = "org.apache.flume.formatter.output.RollTimePathManager$Builder";

		Class c = Class.forName(managerType);
		builderClass = (Class<? extends PathManager.Builder>) c;
		System.out.println(builderClass);

		// PathManager pathController = (PathManager) c.newInstance();
	}

}
