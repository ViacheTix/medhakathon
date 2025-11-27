SELECT район_проживания, COUNT(*) AS cases
FROM patients
GROUP BY район_проживания;