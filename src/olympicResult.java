public class olympicResult {

    // This is a version 2 of this class, compatible with
    // - OlympicMR1
    // - OlympicMR2
    // An object in this class stores the participation and results of a country for one Olympic year

    // declare our data members
    private int totalGold,  totalSilver, totalBronze, totalMedal, totalParticipants, totalEntries;
    private Double sumPercentage, weightPercentage, sumPoints ;

    // Constructor with all parameters
    public olympicResult(int totalGold, int totalSilver , int totalBronze,
                         int totalMedal, Double sumPoints,
                         int totalParticipants, int totalEntries ,
                         Double sumPercentage, Double weightPercentage )
    {

        this.totalGold = totalGold;
        this.totalSilver = totalSilver;
        this.totalBronze = totalBronze;
        this.totalMedal = totalMedal;
        this.sumPoints = sumPoints;
        this.totalParticipants = totalParticipants;
        this.totalEntries= totalEntries;
        this.sumPercentage = sumPercentage;
        this.weightPercentage = weightPercentage;

    }
    // Constructor with simpler version for compatibility
    public olympicResult (int totalGold, int totalSilver , int totalBronze, int totalMedal, Double sumPoints, int totalParticipants, int totalEntries )
    {
        this (totalGold , totalSilver , totalBronze,  totalMedal, sumPoints, totalParticipants, totalEntries, 0.0, 0.0);
    }
    // Constructor with even simpler version for compatibility with OlympicMR1
    public olympicResult(int totalGold, int totalSilver , int totalBronze, int totalMedal, Double sumPoints )
    {
        this (totalGold , totalSilver , totalBronze,  totalMedal, sumPoints, 0, 0,0.0,0.0 );
    }

    // setters
    public void setTotalGold(int totalGold) {
        this.totalGold = totalGold;
    }
    public void setTotalSilver(int totalSilver) {
        this.totalSilver = totalSilver;
    }
    public void setTotalBronze(int totalBronze) {
        this.totalBronze = totalBronze;
    }
    public void setTotalMedal(int totalMedal) {
        this.totalMedal = totalMedal;
    }
    public void setSumPoints(Double sumPoints) {
        this.sumPoints = sumPoints;
    }
    public void setTotalParticipants(int totalParticipants) { this.totalParticipants = totalParticipants; }
    public void setTotalEntries(int totalEntries) { this.totalEntries = totalEntries; }
    public void setSumPercentage(Double sumPercentage) { this.sumPercentage = sumPercentage; }
    public void setWeightPercentage(Double weightPercentage) { this.weightPercentage = weightPercentage; }

    //getters

    public int getTotalGold() {
        return totalGold;
    }
    public int getTotalSilver() {
        return totalSilver;
    }
    public int getTotalBronze () { return totalBronze; }
    public int getTotalMedal() {
        return totalMedal;
    }
    public Double getSumPoints() {
        return sumPoints;
    }
    public int getTotalParticipants() { return totalParticipants; }
    public int getTotalEntries() { return totalEntries; }
    public Double getSumPercentage() { return sumPercentage; }
    public Double getWeightPercentage() {
        return weightPercentage;
    }

    // My methods

    @Override
    public String toString() {
        return "olympicResult{" +
                "totalParticipants='" + totalParticipants+ '\'' +
                ", totalEntries='" + totalEntries+ '\'' +
                ", gold='" + totalGold + '\'' +
                ", silver='" + totalSilver + '\'' +
                ", bronze='" + totalBronze + '\'' +
                ", medals='" + totalMedal + '\'' +
                ", points='" + sumPoints + '\'' +
                ", SumPercentage='" + sumPercentage + '\'' +
                ", WeightPercentage='" + weightPercentage + '\'' +
                '}';
    }
}
