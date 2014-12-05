#!/usr/bin/env ruby

require 'pg'
require 'csv'
require 'pry'

if ARGV.length != 1
  puts "usage: #{__FILE__} \"<actor name>\""
  exit(1)
end

target_actor = ARGV[0]


# =======================

def db_connection
  begin
    connection = PG.connect(dbname: 'movie_casts')
    yield(connection)
  ensure
    connection.close
  end
end

# ARRAY_TO_DB
# ===========
# -- CONN is the connection established in db_connection
# -- TABLE_NAME is the name of the table you are inserting into
# -- COLUMNS input should be an array of strings representing the columns
#    input will be inserted into, in the order they translate from the array
# -- INPUT_ARRAY should be an array of arrays. Each interior array represents a
#    row, with elements in corresponding order to "columns" array

ACTORS_FILENAME = "actors.csv"
MOVIES_FILENAME = "movies.csv"
CASTS_FILENAME = "cast_members.csv"

actors_array = CSV.read(ACTORS_FILENAME)
movies_array = CSV.read(MOVIES_FILENAME)
casts_array = CSV.read(CASTS_FILENAME)

def array_to_db (conn, table_name, columns, input_array)
  input_array.each do |row|
    conn.exec_params("INSERT INTO #{table_name} (#{columns.join(",")}) VALUES ($1, $2)", row)
  end
end

# ------------------------
# READ CSVs into DB
# ------------------------

db_connection do |conn|
  if conn.exec("SELECT count(*) FROM actors")[0]["count"] == 0.to_s
    puts "Adding!"
    array_to_db(conn, "actors", ["id, name"], actors_array)
    array_to_db(conn, "movies", ["id", "name"], movies_array)
    array_to_db(conn, "cast_members", ["actor_id", "movie_id"], casts_array)
  end
end


# ============================
# A*
# ============================

# HEURISTIC: g_score + 1
def get_name(conn, id)
  conn.exec_params("SELECT name FROM actors WHERE id=$1",
    [id])[0]["name"]
end

def get_shared_movie(conn, id1, id2)
  item = conn.exec_params("SELECT movies.name FROM movies
    JOIN cast_members AS actors_1 ON movies.id = actors_1.movie_id
    JOIN cast_members AS actors_2 ON movies.id = actors_2.movie_id
    WHERE actors_1.actor_id = $1 AND actors_2.actor_id = $2",
  [id1, id2])[0]["name"]
  item
end

def print_path(came_from, ending_id)
  db_connection do |conn|
    parent = came_from[ending_id]
    current = ending_id
    while parent != -1
      puts "Actor: #{get_name(conn, current)}"
      puts "Movie: #{get_shared_movie(conn, current, parent)}"
      current = parent
      parent = came_from[current]
    end
    puts "Actor: #{get_name(conn, current)}"
  end
end

def get_connections(conn, id)
  connected_actors = []
  movies = conn.exec_params("SELECT movie_id AS id FROM cast_members
    WHERE actor_id = $1", [id])
  movie_ids = movies.map { |hash| hash["id"].to_i }
  connected_actors = conn.exec_params("SELECT actor_id FROM cast_members
    WHERE movie_id in (#{movie_ids.join(",")}) AND actor_id != $1", [id])
  connected_actors.map { |hash| hash["actor_id"].to_i }.uniq
end

def a_star(start_id, goal_id)
  db_connection do |conn|
    # Initialized SEARCHED_NODES -- array of searched IDs
    searched_nodes = []

    # Initialize OPEN_NODES -- hash of ID: G_SCORE pairings
    # First item has g_score of 0
    open_nodes = {start_id => 0}

    # Initialize CAME_FROM -- hash of ID: PARENT_ID pairings
    # First item is paired with Parent ID -1
    came_from = {start_id => -1}

    i = 0
    # While there are open nodes...
    while !open_nodes.empty?
      puts "i = #{i}"
      i += 1

      # Set CURRENT to the ID of the lowest G_SCORE
      current = open_nodes.min_by { |id, g_score| g_score }[0]

      # If we've found Marky Mark...
      if current.to_i == goal_id
        # Break, and print out how we've gotten here.
        return print_path(came_from, goal_id)
      end

      # Add CURRENT to SEARCHED_NODES
      searched_nodes << current

      # Delete CURRENT from OPEN_NODES
      # current_g_score = open_nodes.delete(current)
      # Break the loop if this node is already 6 deep
      # if current_g_score == 6
      #   puts "found a deep one"
      #   continue
      # end

      # CHILDREN = [find all connected nodes]
      children = get_connections(conn, current)

    #   # For EACH in CHILDREN...
    #   children.each do |child|
    #     # If CHILD is in SEARCHED_NODES
    #     if searched_nodes.include?(child)
    #       next
    #     end
    #
    #     # if (CHILD isn't in OPEN_NODES) || (CURRENT.g_score + 1 < CHILD.g_score)
    #     if !open_nodes.include?(child) || current_g_score + 1 < open_nodes[child]
    #       # CAME_FROM[CHILD] = CURRENT
    #       came_from[child] = current
    #
    #       # G_SCORE[CHILD] = CURRENT.g_score + 1
    #       open_nodes[child] = current_g_score + 1
    #
    #     end
    #   end
    end
  end
  # If nothing worked, return FAILURE
  puts "This actor has no relation to the Funky Bunch."
end


#a_star(12589, 1841)
a_star(40179, 1841)
#a_star(3, 1)
# db_connection do |conn|
#   puts get_shared_movie(conn, 2, 1)[0]
# end
