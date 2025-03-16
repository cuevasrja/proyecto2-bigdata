import mapreduce.ArtistsPopularities;
import mapreduce.ClassifyPopularity;
import mapreduce.RelationPopularity;
import mapreduce.GenresPopularities;

public class App {
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Usage: App <option> <file> <output>");
			System.exit(1);
		}
		int option = Integer.parseInt(args[0]);
		String[] newArgs = new String[args.length - 1];
		System.arraycopy(args, 1, newArgs, 0, args.length - 1);
		switch (option) {
			case 1:
				ClassifyPopularity classifyPopularity = new ClassifyPopularity();
				classifyPopularity.execute(newArgs);
				break;
			case 2:
				ArtistsPopularities artistsPopularities = new ArtistsPopularities();
				artistsPopularities.execute(newArgs);
				break;
			case 3:
				GenresPopularities genresPopularities = new GenresPopularities();
				genresPopularities.run(newArgs);
				break;
			case 4:
				RelationPopularity relationPopularity = new RelationPopularity();
				relationPopularity.execute(newArgs);
				break;
			default:
				System.out.println("Invalid option");
				System.exit(1);
		}
	}
}

