package sample;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.scene.control.ProgressBar;
import javafx.scene.control.TableView;
import sample.model.Album;
import sample.model.Artist;
import sample.model.DataSource;

import java.util.Scanner;


public class Controller {

    @FXML
    private TableView artistTable;

    @FXML
    private ProgressBar progressBar;


    //Why do we use an ObservableList<> and not a normal ArrayList
    @FXML
    public void listArtists() {
        Task<ObservableList<Artist>> task = new GetAllArtistsTask();
        artistTable.itemsProperty().bind(task.valueProperty());
        progressBar.progressProperty().bind(task.progressProperty());

        progressBar.setVisible(true);

        task.setOnSucceeded(event -> progressBar.setVisible(false));
        task.setOnFailed(event -> progressBar.setVisible(false));

        new Thread(task).start();
    }

    @FXML
    public void listAlbumsForArtist() {
        final Artist artist = (Artist) artistTable.getSelectionModel().getSelectedItem();
        if (artist == null) {
            System.out.println("NO ARTIST SELECTED");
            return;
        }
        Task<ObservableList<Album>> task = new Task<ObservableList<Album>>() {

            @Override
            protected ObservableList<Album> call() throws Exception {
                return FXCollections.observableArrayList(
                        DataSource.getInstance().queryAlbumsForArtistId(artist.getId())
                );
            }
        };
        artistTable.itemsProperty().bind(task.valueProperty());
        new Thread(task).start();
    }

    @FXML
    public void updateArtist() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter the id artist id");
        final Artist artist = (Artist) artistTable.getItems().get(scanner.nextInt() - 1);
        System.out.println("Enter the artist's new name:");
        String newName = scanner.next();
        Task<Boolean> task = new Task<Boolean>() {


            @Override
            protected Boolean call() throws Exception {
                if (newName.isEmpty()) {
                    System.out.println("The entered new name is invalid. The name won't be updated!");
                    return DataSource.getInstance().updateArtistName(artist.getId(), artist.getName());
                } else if (newName.equals(artist.getName())) {
                    System.out.println("The new name is as same as the old name!");
                    return DataSource.getInstance().updateArtistName(artist.getId(), artist.getName());
                } else{
                    System.out.printf("The name has been updated from \"%s\" to \"%s\"",artist.getName(),newName);
                    return DataSource.getInstance().updateArtistName(artist.getId(), newName);
                }
            }
        };

        task.setOnSucceeded(event -> {
            if(task.valueProperty().get()){
                artist.setName(newName);
                artistTable.refresh();
            }
        });

        new Thread(task).start();
    }
}

class GetAllArtistsTask extends Task {

    @Override
    protected ObservableList<Artist> call() {
        return FXCollections.observableArrayList
                (DataSource.getInstance().queryArtists(DataSource.ORDER_BY.ASC));
    }
}
