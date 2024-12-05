const express = require("express");
const mysql = require("mysql2");
const { MongoClient, ObjectId } = require("mongodb");
require("dotenv").config();

const app = express();
const port = process.env.PORT;

// Create a connection pool
const pool = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    port: process.env.DB_PORT,
    ssl: process.env.DB_SSL === "true" ? { rejectUnauthorized: true } : false,
});

const uri = process.env.MONGODB_URI;
const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });

// return all columns for the actor table
app.get("/api/v1/actors", (req, res) => {
    pool.query("DESCRIBE actor", (err, results) => {
        if (err) {
            console.error("Error querying database:", err);
            res.status(500).json({
                error: "Error connecting to the database",
                details: err.message,
            });
        } else {
            // Extract column names (Field) into an array
            const columnNames = results.map((column) => column.Field);
            res.status(200).json({
                status: "success",
                columns: columnNames,
            });
        }
    });
});

// return all columns for the customer table
app.get("/api/v1/customers", (req, res) => {
    pool.query("DESCRIBE customer", (err, results) => {
        if (err) {
            console.error("Error querying database:", err);
            res.status(500).json({
                error: "Error connecting to the database",
                details: err.message,
            });
        } else {
            // Extract column names (Field) into an array
            const columnNames = results.map((column) => column.Field);
            res.status(200).json({
                status: "success",
                columns: columnNames,
            });
        }
    });
});

// return all columns for the store table
app.get("/api/v1/stores", (req, res) => {
    pool.query("DESCRIBE store", (err, results) => {
        if (err) {
            console.error("Error querying database:", err);
            res.status(500).json({
                error: "Error connecting to the database",
                details: err.message,
            });
        } else {
            // Extract column names (Field) into an array
            const columnNames = results.map((column) => column.Field);
            res.status(200).json({
                status: "success",
                columnNames,
            });
        }
    });
});

// return all columns for the film table
app.get("/api/v1/films", (req, res) => {
    const query = req.query.query;

    if (query) {
        const sql = "SELECT * FROM film WHERE LOWER(title) LIKE ?";
        const searchValue = `%${query.toLowerCase()}%`;

        pool.query(sql, [searchValue], (err, results) => {
            if (err) {
                console.error("Error querying database:", err);
                res.status(500).json({
                    error: "Error querying the database",
                    details: err.message,
                });
            } else {
                res.status(200).json({
                    status: "success",
                    results,
                });
            }
        });
    } else {
        // Return column names if no query parameter is provided
        pool.query("DESCRIBE film", (err, results) => {
            if (err) {
                console.error("Error querying database:", err);
                res.status(500).json({
                    error: "Error querying the database",
                    details: err.message,
                });
            } else {
                const columnNames = results.map((column) => column.Field);
                res.status(200).json({
                    status: "success",
                    columns: columnNames,
                });
            }
        });
    }
});

// Return a specific actor by ID
app.get("/api/v1/actors/:id", (req, res) => {
    const { id } = req.params;
    pool.query("SELECT * FROM actor WHERE actor_id = ?", [id], (err, results) => {
        if (err) {
            console.error("Error querying database:", err);
            res.status(500).json({
                error: "Error querying the database",
                details: err.message,
            });
        } else if (results.length === 0) {
            res.status(404).json({
                error: "Actor not found",
            });
        } else {
            res.status(200).json({
                status: "success",
                result: results,
            });
        }
    });
});

// Return a specific film by ID
app.get("/api/v1/films/:id", (req, res) => {
    const { id } = req.params;
    pool.query("SELECT * FROM film WHERE film_id = ?", [id], (err, results) => {
        if (err) {
            console.error("Error querying database:", err);
            res.status(500).json({
                error: "Error querying the database",
                details: err.message,
            });
        } else if (results.length === 0) {
            res.status(404).json({
                error: "Film not found",
            });
        } else {
            res.status(200).json({
                status: "success",
                result: results,
            });
        }
    });
});

// Return a specific store by ID
app.get("/api/v1/stores/:id", (req, res) => {
    const { id } = req.params;
    pool.query("SELECT * FROM store WHERE store_id = ?", [id], (err, results) => {
        if (err) {
            console.error("Error querying database:", err);
            res.status(500).json({
                error: "Error querying the database",
                details: err.message,
            });
        } else if (results.length === 0) {
            res.status(404).json({
                error: "Store not found",
            });
        } else {
            res.status(200).json({
                status: "success",
                result: results,
            });
        }
    });
});

// Return a specific customer by ID
app.get("/api/v1/customers/:id", (req, res) => {
    const { id } = req.params;
    pool.query("SELECT * FROM customer WHERE customer_id = ?", [id], (err, results) => {
        if (err) {
            console.error("Error querying database:", err);
            res.status(500).json({
                error: "Error querying the database",
                details: err.message,
            });
        } else if (results.length === 0) {
            res.status(404).json({
                error: "Customer not found",
            });
        } else {
            res.status(200).json({
                status: "success",
                result: results,
            });
        }
    });
});

// Return all films for a specified actor
app.get("/api/v1/actors/:id/films", (req, res) => {
    const { id } = req.params;

    // SQL query to retrieve all films for the specified actor
    const sql = `
        SELECT f.* 
        FROM film AS f
        JOIN film_actor AS fa ON fa.film_id = f.film_id
        WHERE fa.actor_id = ?
    `;

    pool.query(sql, [id], (err, results) => {
        if (err) {
            console.error("Error querying database:", err);
            res.status(500).json({
                error: "Error querying the database",
                details: err.message,
            });
        } else if (results.length === 0) {
            res.status(404).json({
                error: "No films found for this actor",
            });
        } else {
            res.status(200).json({
                status: "success",
                films: results,
            });
        }
    });
});

// Return all actors for a specified film
app.get("/api/v1/films/:id/actors", (req, res) => {
    const { id } = req.params;

    // SQL query to retrieve all actors for the specified film
    const sql = `
        SELECT a.*
        FROM actor AS a
        JOIN film_actor AS fa ON fa.actor_id = a.actor_id
        WHERE fa.film_id = ?
    `;

    pool.query(sql, [id], (err, results) => {
        if (err) {
            console.error("Error querying database:", err);
            res.status(500).json({
                error: "Error querying the database",
                details: err.message,
            });
        } else if (results.length === 0) {
            res.status(404).json({
                error: "No actors found for this film",
            });
        } else {
            res.status(200).json({
                status: "success",
                actors: results,
            });
        }
    });
});

// Return film details from the film_list view for the specified film
app.get("/api/v1/films/:id/detail", (req, res) => {
    const { id } = req.params;

    const sql = `
        SELECT *
        FROM film_list
        WHERE film_id = ?
    `;

    pool.query(sql, [id], (err, results) => {
        if (err) {
            console.error("Error querying database:", err);
            res.status(500).json({
                error: "Error querying the database",
                details: err.message,
            });
        } else if (results.length === 0) {
            res.status(404).json({
                error: "Film not found in film_list",
            });
        } else {
            res.status(200).json({
                status: "success",
                film_details: results[0],
            });
        }
    });
});

// Return customer details from the customer_list view for the specified customer
app.get("/api/v1/customers/:id/detail", (req, res) => {
    const { id } = req.params;

    // SQL query to retrieve customer details from the customer_list view
    const sql = `
        SELECT *
        FROM customer_list
        WHERE customer_id = ?
    `;

    pool.query(sql, [id], (err, results) => {
        if (err) {
            console.error("Error querying database:", err);
            res.status(500).json({
                error: "Error querying the database",
                details: err.message,
            });
        } else if (results.length === 0) {
            res.status(404).json({
                error: "Customer not found in customer_list",
            });
        } else {
            res.status(200).json({
                status: "success",
                customer_details: results[0],
            });
        }
    });
});

// Return actor details from the actor_info view for the specified actor
app.get("/api/v1/actors/:id/detail", (req, res) => {
    const { id } = req.params;

    // SQL query to retrieve actor details from the actor_info view
    const sql = `
        SELECT *
        FROM actor_info
        WHERE actor_id = ?
    `;

    pool.query(sql, [id], (err, results) => {
        if (err) {
            console.error("Error querying database:", err);
            res.status(500).json({
                error: "Error querying the database",
                details: err.message,
            });
        } else if (results.length === 0) {
            res.status(404).json({
                error: "Actor not found in actor_info",
            });
        } else {
            res.status(200).json({
                status: "success",
                actor_details: results[0],
            });
        }
    });
});

// Return inventory ids in stock for a specified film and store using the film_in_stock stored procedure
app.get("/api/v1/inventory-in-stock/:film_id/:store_id", (req, res) => {
    const { film_id, store_id } = req.params;

    // SQL query to call the stored procedure `film_in_stock`
    const sql = "CALL film_in_stock(?, ?)";

    pool.query(sql, [film_id, store_id], (err, results) => {
        if (err) {
            console.error("Error querying database:", err);
            res.status(500).json({
                error: "Error executing stored procedure",
                details: err.message,
            });
        } else if (results[0].length === 0) {
            res.status(404).json({
                error: "No inventory found for the specified film and store",
            });
        } else {
            const inventoryIds = results[0].map(row => row.inventory_id);
            res.status(200).json({
                status: "success",
                inventory_ids: inventoryIds,
            });
        }
    });
});

// Movie API in MongoDB
async function MovieRun() {
    try {
        // Connect the client to the MongoDB server
        await client.connect();
        console.log("Connected to MongoDB");

        // Access the 'sample_mflix' database and 'movies' collection
        const db = client.db("sample_mflix");
        const moviesCollection = db.collection("movies");

        app.get("/api/v1/movies", async (req, res) => {
            const filters = {};

            // Optional query parameters for filtering
            const { genre, year, director } = req.query;

            if (genre) {
                filters.genres = { $in: [genre] };
            }
            if (year) {
                filters.year = parseInt(year);
            }
            if (director) {
                filters.directors = { $in: [director] };
            }

            const queryOptions = {
                limit: 10,
            };

            try {
                const movies = await moviesCollection.find(filters, queryOptions).toArray();
                res.status(200).json(movies);
            } catch (error) {
                console.error("Error querying MongoDB:", error);
                res.status(500).json({
                    error: "Error querying the database",
                    details: error.message,
                });
            }
        });
    } catch (error) {
        console.error("Error connecting to MongoDB:", error);
    }
}

// Colors API in MongoDB
async function ColorRun() {
    try {
        // Connect the client to the MongoDB server
        await client.connect();
        console.log("Connected to MongoDB");

        const db = client.db("cs480-project2");
        const colorsCollection = db.collection("colors");

        // 1. GET /colors - Return all fields for all documents
        app.get("/api/v1/colors", async (req, res) => {
            try {
                const colors = await colorsCollection.find().toArray();
                res.status(200).json(colors);
            } catch (error) {
                console.error("Error querying MongoDB:", error);
                res.status(500).json({
                    error: "Error querying the database",
                    details: error.message,
                });
            }
        });

        // 2. POST /colors - Insert a new item, return the result
        app.post("/api/v1/colors", async (req, res) => {
            const newColor = req.body;

            try {
                const result = await colorsCollection.insertOne(newColor);
                res.status(201).json({
                    message: "Color added successfully",
                    insertedId: result.insertedId,
                    color: newColor,
                });
            } catch (error) {
                console.error("Error inserting data into MongoDB:", error);
                res.status(500).json({
                    error: "Error inserting data into the database",
                    details: error.message,
                });
            }
        });

        // 3. GET /colors/<id> - Return all fields for the specified document
        app.get("/api/v1/colors/:id", async (req, res) => {
            const { id } = req.params;
            const objectId = new ObjectId(id); // Ensure you use `new ObjectId(id)` to instantiate

            try {
                const color = await colorsCollection.findOne({ _id: objectId });
                if (!color) {
                    res.status(404).json({ error: "Color not found" });
                } else {
                    res.status(200).json(color);
                }
            } catch (error) {
                console.error("Error querying MongoDB:", error);
                res.status(500).json({
                    error: "Error querying the database",
                    details: error.message,
                });
            }
        });

        // 4. PUT /colors/<id> - Update the specified document, return result
        app.put("/api/v1/colors/:id", async (req, res) => {
            const { id } = req.params;
            const objectId = new ObjectId(id); // Again, ensure proper instantiation of ObjectId
            const updatedColor = req.body;

            try {
                const result = await colorsCollection.updateOne(
                    { _id: objectId },
                    { $set: updatedColor }
                );
                if (result.matchedCount === 0) {
                    res.status(404).json({ error: "Color not found" });
                } else {
                    res.status(200).json({
                        message: "Color updated successfully",
                        updatedColor: updatedColor,
                    });
                }
            } catch (error) {
                console.error("Error updating data in MongoDB:", error);
                res.status(500).json({
                    error: "Error updating data in the database",
                    details: error.message,
                });
            }
        });

        // 5. DELETE /colors/<id> - Delete the specified document, return result
        app.delete("/api/v1/colors/:id", async (req, res) => {
            const { id } = req.params;
            const objectId = new ObjectId(id);

            try {
                const result = await colorsCollection.deleteOne({ _id: objectId });
                if (result.deletedCount === 0) {
                    res.status(404).json({ error: "Color not found" });
                } else {
                    res.status(200).json({
                        message: "Color deleted successfully",
                        deletedId: id,
                    });
                }
            } catch (error) {
                console.error("Error deleting data in MongoDB:", error);
                res.status(500).json({
                    error: "Error deleting data from the database",
                    details: error.message,
                });
            }
        });
    } catch (error) {
        console.error("Error connecting to MongoDB:", error);
    }
}

// Run the server
ColorRun().catch(console.error);
MovieRun().catch(console.error);


// Hello World!
app.get("/", (req, res) => {
    res.send("Hello World!");
});

// Start the server
app.listen(port, () => {
    console.log(`Server is running on port ${port}`);
});
